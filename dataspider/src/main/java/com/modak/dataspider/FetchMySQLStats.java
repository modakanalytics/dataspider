/*Copyright 2018 modakanalytics.com

        Licensed under the Apache License, Version 2.0 (the "License");
        you may not use this file except in compliance with the License.
        You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

        Unless required by applicable law or agreed to in writing, software
        distributed under the License is distributed on an "AS IS" BASIS,
        WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
        See the License for the specific language governing permissions and
        limitations under the License.*/
package com.modak.dataspider;

import com.modak.dataspider.dbutils.LoggerConnectionErrors;
import com.modak.dataspider.dbutils.LoggerUtils;
import com.modak.dataspider.dbutils.ModakQueryRunner;
import com.modak.dataspider.utils.JSONUtils;
import com.modak.dataspider.utils.RSAEncryptionUtils;
import org.apache.commons.dbutils.handlers.ArrayListHandler;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STGroupFile;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * This class will connect to MySQL sources and fetch the metadata from MySQL source and inserts the same into staging tables.
 *@author modakanalytics
 * @version 1.0
 *
 */
public class FetchMySQLStats extends MetadataFetcher {

    protected ConnectAttributes connectAttributes = null;
    protected HashMap<String, String> koshAttributes = null;
    protected HashMap<String, String> runAttributes = null;
    protected long crawljobno;
    private static final org.apache.logging.log4j.Logger LOGGER = LogManager.getLogger("FetchMySQLStats");

    static boolean isDriverLoaded = false;

    @Override
    /**
     * this method will connect to MySql database with provided configurations
     */
    public void run() {
        Connection connectionObj_source = null;
        Connection connectionObj_kosh = null;

        ModakQueryRunner queryRunner = new ModakQueryRunner();
        int isErrored = 0;
        try {
            try {
                // Connection objects to Kosh and source DBs
                connectionObj_kosh = DriverManager.getConnection(koshAttributes.get("jdbc_url"),
                    koshAttributes.get("username"), koshAttributes.get("passwd"));
                connectionObj_source = DriverManager.getConnection(connectAttributes.get("jdbc_url").toString(),
                    connectAttributes.get("username").toString(), connectAttributes.get("passwd").toString());
                //if connection object is null then insert into error logs
                if (connectionObj_kosh == null) {
                    LOGGER.error("Couldn't create connection object to Kosh");
                    LoggerConnectionErrors.insertLog(connectionObj_kosh,
                        Long.parseLong(connectAttributes.get("datastore_id").toString()),
                        "Couldn't create connection object to Kosh", crawljobno, crawljobno - 1,
                        runAttributes.get("resources_path"));
                }
                if (connectionObj_source == null) {
                    LOGGER.error("Couldn't create connection object to Source");
                    LoggerConnectionErrors.insertLog(connectionObj_kosh,
                        Long.parseLong(connectAttributes.get("datastore_id").toString()),
                        "Couldn't create connection object to Source database with URL: "
                            + connectAttributes.get("jdbc_url").toString(),
                        crawljobno, crawljobno - 1, runAttributes.get("resources_path"));
                }
                //if exception caught, insert into log table
            }
            catch (SQLException se) {
                isErrored = 1;
                se.printStackTrace();
                LOGGER.error("Error while creating connection objects to Kosh and Source database with URL:"
                    + connectAttributes.get("jdbc_url").toString() + se);
                LoggerConnectionErrors.insertLog(connectionObj_kosh,
                    Long.parseLong(connectAttributes.get("datastore_id").toString()),
                    "Couldn't create connection object to Source database with URL: "
                        + connectAttributes.get("jdbc_url").toString(),
                    crawljobno, crawljobno - 1, runAttributes.get("resources_path"));
            }
            //closing the open connections
            finally {
                if (isErrored == 1) {
                    if (connectionObj_source != null) {
                        try {
                            connectionObj_source.close();
                        }
                        catch (SQLException e1) {
                            e1.printStackTrace();
                            LOGGER.error("Error while closing the source connection " + e1);
                        }
                    }
                    if (connectionObj_kosh != null) {
                        try {
                            connectionObj_kosh.close();
                        }
                        catch (SQLException e1) {
                            e1.printStackTrace();
                            LOGGER.error("Error while closing the Kosh connection " + e1);
                        }
                    }
                    return;
                }
            }
            // for transaction integrity
            connectionObj_source.setAutoCommit(false);
            // user defined String template for use in DB Crawler program
            STGroup SQLgroup = new STGroupFile(runAttributes.get("resources_path") + File.separator + "templates"
                + File.separator + "mysql_stats.stg", '$', '$');
            STGroup SQLgroup_Kosh = new STGroupFile(runAttributes.get("resources_path") + File.separator + "templates"
                + File.separator + "kosh_sqls.stg", '$', '$');
            LOGGER.info("Started crawling for source id : " + connectAttributes.get("datastore_id"));

            // get table metadata ST file and execute it
            List<Object[]> list_of_dba_all_tables = customSQLSelectExecutor("table_metadata", SQLgroup,
                "query_dba_all_tables", queryRunner, connectionObj_source, SQLgroup_Kosh, connectionObj_kosh);

            // get all_tables_cols ST file and execute it
            List<Object[]> list_of_all_tables_cols = customSQLSelectExecutor("columns_metadata", SQLgroup,
                "query_all_tables_cols", queryRunner, connectionObj_source, SQLgroup_Kosh, connectionObj_kosh);

            // *** inserting the data into the staging tables one after other

            // Batch inserts of table metadata from source into Kosh DB
            customSQLInsertExecutor("table_metadata", list_of_dba_all_tables, "stagingInsert_TableMetadataFromSource_mysql",
                SQLgroup_Kosh, queryRunner, connectionObj_kosh);

            // Batch inserts of column metadata from source into Kosh DB
            customSQLInsertExecutor("columns_metadata", list_of_all_tables_cols, "stagingInsert_ColumnsDataFromSource",
                SQLgroup_Kosh, queryRunner, connectionObj_kosh);

            // connectionObj_kosh.commit();
            connectionObj_kosh.setAutoCommit(true);
            LOGGER.info("Finished crawling for source id : " + connectAttributes.get("datastore_id"));

            // update into status file upon completion
            File finish_logger_file = new File("finish_logger_file.txt");
            FileUtils.writeStringToFile(finish_logger_file,
                connectAttributes.get("datastore_id") + "," + connectAttributes.get("schema_name") + "\n",
                Charset.defaultCharset(), true);

        }
        catch (Exception ex) {
            isErrored = 1;
            ex.printStackTrace();
            LOGGER.error("For datastore_id = " + connectAttributes.get("datastore_id").toString()
                + ", Error while running crawler\n" + ex);
            LoggerUtils.insertLog(
                this.getClass().getSimpleName(), "For datastore_id = "
                    + connectAttributes.get("datastore_id").toString() + ", Error while running crawler\n" + ex,
                false);

            // error logs into an error file
            File error_logger_file = new File("error_logger.txt");
            try {
                FileUtils.writeStringToFile(error_logger_file,
                    connectAttributes.get("datastore_id").toString() + ","
                        + connectAttributes.get("schema_name").toString() + "\n",
                    Charset.defaultCharset(), true);
            }
            catch (IOException e1) {
                e1.printStackTrace();
            }
        }
        finally {
            //closing the open connections
            if (connectionObj_source != null) {
                try {
                    connectionObj_source.close();
                }
                catch (SQLException e1) {
                    e1.printStackTrace();
                    LOGGER.error("Error while closing the source connection " + e1);
                }
            }
            if (connectionObj_kosh != null) {
                try {
                    connectionObj_kosh.close();
                }
                catch (SQLException e1) {
                    e1.printStackTrace();
                    LOGGER.error("Error while closing the Kosh connection " + e1);
                }
            }
            if (isErrored == 1) {
                return;
            }
        }
    }

    /**
     * this method will fetch the connection config from run and kosh attributes and create a connection string as
     * connection attributes
     *
     * @param connectAttributes connection attributes
     * @param koshAttributes    kosh configurations
     * @param runAttributes     run attributes
     * @param crawljobno        current crawl_job _no for the run
     */

    @Override
    public void init(ConnectAttributes connectAttributes, HashMap<String, String> koshAttributes,
        HashMap<String, String> runAttributes, long crawljobno) {
        try {
            this.connectAttributes = connectAttributes;
            this.koshAttributes = koshAttributes;
            this.runAttributes = runAttributes;
            this.crawljobno = crawljobno;
            String hostname = connectAttributes.get("host");
            String connection_template = connectAttributes.get("connection_template");
            String database_name = connectAttributes.get("schema_name").toString();
            String jsonString = null;
            try {
                //fetching the json for connection configuartions
                File stage_file = new File(runAttributes.get("resources_path") + File.separator + "connection_profiles"
                    + File.separator + connection_template + ".json");
                jsonString = FileUtils.readFileToString(stage_file, Charset.defaultCharset());
            }
            catch (FileNotFoundException fne) {
                fne.printStackTrace();
                LOGGER.error("File " + connection_template + ".json is not found\n" + fne);
                LoggerUtils.insertLog(this.getClass().getSimpleName(), "File runAttributes.json is not found\n" + fne,
                    true);
            }
            catch (IOException io) {
                io.printStackTrace();
                LOGGER.error("IOException while reading " + connection_template + ".json\n" + io);
                LoggerUtils.insertLog(this.getClass().getSimpleName(),
                    "IOException while reading " + connection_template + ".json\n" + io, true);
            }
            catch (Exception ex) {
                ex.printStackTrace();
                LOGGER.error("Error while reading the " + connection_template + ".json file\n" + ex);
                LoggerUtils.insertLog(this.getClass().getSimpleName(),
                    "Error while reading the " + connection_template + ".json file\n" + ex, true);
            }
            //fetching the user name and password and decrypting the password as secret key provided.
            Map<String, Object> connection_profile_map = JSONUtils.jsonToMap(jsonString);
            //retrieving the Configuration for connection
            String userName = connection_profile_map.get("username").toString();
            String password = connection_profile_map.get("passwd").toString();
            //check for the jdbc driver in connection_profile
            if (connection_profile_map.containsKey("encrypted_passwd")
                && connection_profile_map.get("encrypted_passwd").toString().equals("Y")) {
                password = RSAEncryptionUtils.decryptPassword(password, runAttributes.get("resources_path") + File.separator + "privateKey");
            }
            if (!isDriverLoaded) {
                Class.forName(connection_profile_map.get("jdbc_driver").toString());
            }
            String connection_string = connection_profile_map.get("jdbc_url").toString();
            connection_string = connection_string.replace("$HOSTNAME$", hostname);
            connection_string = connection_string.replace("$DATABASE_NAME$", database_name.replace("'", ""));

            connectAttributes.put("jdbc_url", connection_string);
            connectAttributes.put("username", userName);
            connectAttributes.put("passwd", password);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            LOGGER.error("Error while calling init() method of FetchMySQLStats\n" + ex);
            LoggerUtils.insertLog(this.getClass().getSimpleName(),
                "Error while calling init() method of FetchMySQLStats\n" + ex, true);
        }
    }

    @Override
    public ArrayList<HashMap<String, String>> getMetaData() {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * this particular method will execute the query and return the outcome of it.
     *
     * @param runAttributeName
     * @param SQLgroup             Template group name
     * @param query_source         template name
     * @param queryRunner          instance of Query runner
     * @param connectionObj_source connection object for source connection
     * @param SQLgroup_Kosh        kosh sql Template group name
     * @param connectionObj_kosh   connection object for kosh connection
     * @return
     */
    public List<Object[]> customSQLSelectExecutor(String runAttributeName, STGroup SQLgroup, String query_source,
        ModakQueryRunner queryRunner, Connection connectionObj_source, STGroup SQLgroup_Kosh,
        Connection connectionObj_kosh) {
        List<Object[]> list_of_Objects_array = null;
        if (runAttributes.containsKey(runAttributeName) && runAttributes.get(runAttributeName).equals("Y")) {
            ST query_sourceST = SQLgroup.getInstanceOf(query_source);
            query_sourceST.add("connectProperties", connectAttributes);
            // execute the SQL
            try {
                list_of_Objects_array = queryRunner.query(connectionObj_source, query_sourceST.render(),
                    new ArrayListHandler(), true);
            }
            catch (Exception ex) {
                ex.printStackTrace();
                LOGGER.error("For datastore_id = " + connectAttributes.get("datastore_id").toString()
                    + ", Error while running insert into staging tables " + query_sourceST.render());
                LoggerUtils.insertLog(this.getClass().getSimpleName(),
                    "For datastore_id = " + connectAttributes.get("datastore_id").toString()
                        + ", Error while running insert into staging tables " + query_sourceST.render() + "\n"
                        + ex,
                    false);

                // append errors to an error file
                try {
                    FileUtils.writeStringToFile(new File(query_source + "_SQLerrors.txt"),
                        ex.fillInStackTrace().toString(), Charset.defaultCharset(), true);
                }
                catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return list_of_Objects_array;
    }

    /**
     * this particular method will execute the query and return the outcome of it.
     *
     * @param runAttributeName
     * @param list_of_Objects_array result of query runner
     * @param insertSQLST_name      template name
     * @param SQLgroup_Kosh         Template group name
     * @param queryRunner           instance of Query runner
     * @param connectionObj_kosh    connection object for kosh connection
     */

    public void customSQLInsertExecutor(String runAttributeName, List<Object[]> list_of_Objects_array,
        String insertSQLST_name, STGroup SQLgroup_Kosh, ModakQueryRunner queryRunner,
        Connection connectionObj_kosh) {
        if (list_of_Objects_array != null) {
            ST insertSQL_ST = SQLgroup_Kosh.getInstanceOf(insertSQLST_name);
            // convert List<Object[]> to Object[][] for making it use in batch
            // inserts
            Object[][] table_metadata_dataArray = new Object[list_of_Objects_array.size()][];
            int i = 0;
            for (Object[] row : list_of_Objects_array) {
                table_metadata_dataArray[i++] = row;
            }
            try {
                //batch execution of queries
                queryRunner.batch(connectionObj_kosh, insertSQL_ST.render(), table_metadata_dataArray);
            }
            catch (Exception ex) {
                ex.printStackTrace();
                //insert the exception in to log table
                LOGGER.error("For datastore_id = " + connectAttributes.get("datastore_id").toString()
                    + ", Error while running insert into staging tables " + insertSQL_ST.render());
                LoggerUtils.insertLog(this.getClass().getSimpleName(),
                    "For datastore_id = " + connectAttributes.get("datastore_id").toString()
                        + ", Error while running insert into staging tables " + insertSQL_ST.render() + "\n"
                        + ex,
                    false);

                // append errors to an error file
                try {
                    FileUtils.writeStringToFile(new File(runAttributeName + "_InsertErrors.txt"),
                        ex.fillInStackTrace().toString(), Charset.defaultCharset(), true);
                }
                catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }
}
