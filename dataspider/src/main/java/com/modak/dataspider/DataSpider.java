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

import com.modak.dataspider.dbutils.ConnectionManagerKosh;
import com.modak.dataspider.dbutils.LoggerUtils;
import com.modak.dataspider.dbutils.ModakQueryRunner;
import com.modak.dataspider.utils.JSONUtils;
import com.modak.dataspider.utils.RSAEncryptionUtils;
import org.apache.commons.cli.*;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
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
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This DataSpider class is the entry point for the Kosh Metadata Crawler project
 * This class connects to the Kosh DB and gets the source information to be
 * crawled and then fetches metadata for the source and then saves it back into Kosh Metadata repository tables.
 *
 * @author modakanalytics
 * This file is subject to the terms and conditions defined with Modak Analytics', which is part of the
 *         project agreement. Copyright Modak Analytics(c)
 *
 * @author  modakanalytics on 2018/01/29 added the data_spider_verification logic
 * @version 1.0
 **
 */

public class DataSpider {

    protected MetadataFetcherFactory fetcherFactory = null;
    HashMap<String, String> koshAttributes = new HashMap<String, String>();
    HashMap<String, String> runAttributes = new HashMap<String, String>();
    static String resources_path = null;
    static int threads = 0;
    static String runningSource = null;
    // crawl job no related info
    int crawl_id;
    List<Map<String, Object>> crawlJobNoLMap = null;
    private static final org.apache.logging.log4j.Logger LOGGER = LogManager.getLogger("DataSpider");
    STGroup SQLgroup_Kosh = new STGroupFile(
        resources_path + File.separator + "templates" + File.separator + "kosh_sqls.stg", '$', '$');
    STGroup SQLgroup_RollBack = new STGroupFile(
        resources_path + File.separator + "templates" + File.separator + "roll_back_sqls.stg", '$', '$');

    /**
     * Constructor for data spider
     */
    public DataSpider() {
        LOGGER.info("DataSpider has got instantiated");
    }


    //initialize the Kosh Crawler
    public void init() {
        try {
            // loading Kosh related JSON file
            try {
                Map<String, Object> connection_profile_map = JSONUtils.jsonToMap(FileUtils.readFileToString(
                    new File(
                        resources_path + File.separator + "connection_profiles" + File.separator + "kosh.json"),
                    Charset.defaultCharset()));
                String password = connection_profile_map.get("passwd").toString();
                if (connection_profile_map.containsKey("encrypted_passwd")
                    && connection_profile_map.get("encrypted_passwd").toString().equals("Y")) {
                    password = RSAEncryptionUtils.decryptPassword(password, resources_path + File.separator + "privateKey");
                }
                Class.forName(connection_profile_map.get("jdbc_driver").toString());
                koshAttributes.put("passwd", password);
                koshAttributes.put("jdbc_url", connection_profile_map.get("jdbc_url").toString());
                koshAttributes.put("username", connection_profile_map.get("username").toString());
            }
            catch (FileNotFoundException fne) {
                fne.printStackTrace();
                LOGGER.error("File kosh.json is not found" + fne);
                LoggerUtils.insertLog(this.getClass().getSimpleName(), "File kosh.json is not found\n" + fne, true);
            }
            catch (IOException io) {
                io.printStackTrace();
                LOGGER.error("IOException while reading file kosh.json" + io);
                LoggerUtils.insertLog(this.getClass().getSimpleName(),
                    "IOException while reading file kosh.json\n" + io, true);
            }
            catch (Exception ex) {
                ex.printStackTrace();
                LOGGER.error("Error while reading the file kosh.json file" + "\n" + ex);
                LoggerUtils.insertLog(this.getClass().getSimpleName(), "Error while reading the file kosh.json\n" + ex,
                    true);
            }

            // loading the runAttribute.JSON file
            try {
                runAttributes = new HashMap<String, String>();
                Map<String, Object> mapping = JSONUtils.jsonToMap(FileUtils.readFileToString(new File(
                        resources_path + File.separator + "run_configurations" + File.separator + "runAttributes.json"), Charset.defaultCharset()));
                mapping.forEach((k, v) -> runAttributes.put(k, emptyIfNull(v).toString()));
                threads = Integer.parseInt(runAttributes.get("threads").toString());
                // adding resource path to the runAttributes map
                runAttributes.put("resources_path", resources_path);


            }
            catch (NumberFormatException nfe) {
                nfe.printStackTrace();
                LOGGER.error("threads attributes in runAttributes.json is non integer value\n" + nfe);
                LoggerUtils.insertLog(this.getClass().getSimpleName(),
                    "threads attributes in runAttributes.json is non integer value\n" + nfe, true);
            }
            catch (FileNotFoundException fne) {
                fne.printStackTrace();
                LOGGER.error("File runAttributes.json is not found\n" + fne);
                LoggerUtils.insertLog(this.getClass().getSimpleName(), "File runAttributes.json is not found\n" + fne,
                    true);
            }
            catch (IOException io) {
                io.printStackTrace();
                LOGGER.error("IOException while reading runAttributes.json\n" + io);
                LoggerUtils.insertLog(this.getClass().getSimpleName(),
                    "IOException while reading runAttributes.json\n" + io, true);
            }
            catch (Exception ex) {
                ex.printStackTrace();
                LOGGER.error("Error while reading the runAttributes.json file\n" + ex);
                LoggerUtils.insertLog(this.getClass().getSimpleName(),
                    "Error while reading the runAttributes.json file\n" + ex, true);
            }

            //creating the instance of MetadataFetcherFactory
            fetcherFactory = new MetadataFetcherFactory();

            //initialize the metadataFetcherFactory
            fetcherFactory.init(new HashMap<String, String>(koshAttributes), new HashMap<String, String>(runAttributes),
                resources_path);

        }
        catch (Exception ex) {
            ex.printStackTrace();
            LOGGER.error("Error while running init() for " + this.getClass().getSimpleName() + "\n" + ex);
            LoggerUtils.insertLog(this.getClass().getSimpleName(),
                "Error while running init() for " + this.getClass().getSimpleName() + "\n" + ex, true);
        }
    }

    // roll back
    public void rollBack(Connection connection_kosh) {
        LOGGER.info("Checking if roll back required ");
        QueryRunner queryrunner = new QueryRunner();
        try {
            // requireRollBackCrawlJobInfo
            Map<String, Object> mapgetcrawljobno = null;
            ST requireRollBackCrawlJobInfo = SQLgroup_RollBack.getInstanceOf("requireRollBackCrawlJobInfo");
            mapgetcrawljobno = queryrunner.query(connection_kosh, requireRollBackCrawlJobInfo.render(),
                new MapHandler());
            LOGGER.info(requireRollBackCrawlJobInfo.render());
            LOGGER.info("crawl_id to be roll backed : " + mapgetcrawljobno.get("crawl_id"));
            // RollBackCrawlJobInfo
            if (mapgetcrawljobno.get("crawl_id") != null) {
                Map<String, Object> maprollBackCrawlJobInfo = null;
                ST rollBackCrawlJobInfo = SQLgroup_RollBack.getInstanceOf("RollBackCrawlJobInfo");
                rollBackCrawlJobInfo.add("rollbackjobno", mapgetcrawljobno);
                maprollBackCrawlJobInfo = queryrunner
                    .query(connection_kosh, rollBackCrawlJobInfo.render(), new MapHandler());
                LOGGER.info(rollBackCrawlJobInfo.render());
                mapgetcrawljobno.put("prev_crawl_id", maprollBackCrawlJobInfo.get("prev_crawl_id"));
                ST rollbackTableUpdate = SQLgroup_RollBack.getInstanceOf("rollbackTableUpdate");
                rollbackTableUpdate.add("mapgetcrawljobno", mapgetcrawljobno);
                LOGGER.info(rollbackTableUpdate.render());
                queryrunner.update(connection_kosh, rollbackTableUpdate.render());
                ST rollbackTableDelete = SQLgroup_RollBack.getInstanceOf("rollbackTableDelete");
                rollbackTableDelete.add("mapgetcrawljobno", mapgetcrawljobno);
                LOGGER.info(rollbackTableDelete.render());
                queryrunner.update(connection_kosh, rollbackTableDelete.render());
                // delete query
                ST delete_query = SQLgroup_RollBack.getInstanceOf("delete_query");
                delete_query.add("rollbackjobno", mapgetcrawljobno);
                queryrunner.update(connection_kosh, delete_query.render());
            }
            else {
                LOGGER.info("Roll back not required continue with crawling");
            }
        }
        catch (NullPointerException ex) {
            ex.printStackTrace();
            LOGGER.error("Failed due to " + ex);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            LOGGER.error("Failed due to " + ex);
        }
    }

    //crawling the data
    public void doCrawling(Connection connectionObj_kosh) {
        LOGGER.info("DataSpider crawling started !");
        long t1 = System.currentTimeMillis();
        try {
            QueryRunner queryRunner = new QueryRunner();
            // get the new crawl job number
            ST getNewCrawlJobInfo = SQLgroup_Kosh.getInstanceOf("getNewCrawlJobInfo");
            crawlJobNoLMap = new ModakQueryRunner()
                .query(connectionObj_kosh, getNewCrawlJobInfo.render(), new MapListHandler());
            crawl_id = Integer.parseInt(crawlJobNoLMap.get(0).get("crawl_id").toString());
            // insert into crawljobinfo table with crawljob number
            ST insertCrawlJobInfo = SQLgroup_Kosh.getInstanceOf("insertCrawlJobInfo");
            insertCrawlJobInfo.add("crawl_id", crawl_id);
            queryRunner.update(connectionObj_kosh, insertCrawlJobInfo.render());

            // truncate staging and make it ready for fresh crawling
            connectionObj_kosh.setAutoCommit(false);
            ST stagingTruncate_Batch = SQLgroup_Kosh.getInstanceOf("stagingTruncate_Batch");
            new ModakQueryRunner().update(connectionObj_kosh, stagingTruncate_Batch.render());
            connectionObj_kosh.commit();
            // Thread Executor service with number of threads from runAttributes.json
            ExecutorService taskExecutor = Executors.newFixedThreadPool(threads);

            // getting source metadata information for the sources
            ST getSourceMetaData = SQLgroup_Kosh.getInstanceOf("getSourceMetaData_" + runningSource);
            LOGGER.info("DataSpider Started Crawling at this time : " + new Date(System.currentTimeMillis()));
            List<Map<String, Object>> list_of_sources = queryRunner.query(connectionObj_kosh,
                getSourceMetaData.render(), new MapListHandler());
            LOGGER.info("No. of sources picked for crawling : " + list_of_sources.size());
            for (Map<String, Object> map : list_of_sources) {
                ConnectAttributes connectAttributes = new ConnectAttributes();
                map.forEach((k, v) -> connectAttributes.put(k, emptyIfNull(v).toString()));
                MetadataFetcher fetcher = fetcherFactory.getInstance(connectAttributes, crawl_id);
                if (fetcher.equals(null)) {
                    LOGGER.info("Fetcher is Null");
                }
                taskExecutor.execute(fetcher);
            }
            // shutting down the task executor
            taskExecutor.shutdown();
            try {
                taskExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
                LOGGER.error("Task executor iterrupted Exception" + "\n" + e);
                LoggerUtils.insertLog(this.getClass().getSimpleName(), "Task executor iterrupted Exception" + "\n" + e,
                    false);
            }
            long t2 = System.currentTimeMillis();
            LOGGER.info(
                "DataSpider Crawling is completed in : " + TimeUnit.MILLISECONDS.toMinutes((t2 - t1)) + " Minutes");
            LOGGER.info("DataSpider Ended Crawling at " + new Date(t2));
        }
        catch (Exception ex) {
            ex.printStackTrace();

            //closing the connection
            if (connectionObj_kosh != null) {
                try {
                    connectionObj_kosh.close();
                }
                catch (SQLException e) {
                    e.printStackTrace();
                    LOGGER.error("Error while closing the Kosh connection\n" + e);
                    LoggerUtils.insertLog(this.getClass().getSimpleName(),
                        "Error while closing the Kosh connection\n" + e, false);
                }
            }
            LOGGER.error("Error while doing Kosh Crawling !");
            LoggerUtils.insertLog(this.getClass().getSimpleName(), "Error while doing Kosh Crawling !", true);
        }
    }

    /*
     * This method does CDC of the tables from staging and inserts into main
     * tables
     */

    public void doCDC(Connection connectionObj_kosh) {
        long t1 = System.currentTimeMillis();
        LOGGER.info("Kosh CDC started at : " + new Date(t1));
        int isErrored = 0;
        ModakQueryRunner queryRunner = new ModakQueryRunner();
        try {

            connectionObj_kosh.setAutoCommit(false);
            ST getCrawlJobInfo = SQLgroup_Kosh.getInstanceOf("getCrawlJobInfo");
            crawlJobNoLMap = new ModakQueryRunner().query(connectionObj_kosh, getCrawlJobInfo.render(),
                    new MapListHandler());
            crawl_id = Integer.parseInt(crawlJobNoLMap.get(0).get("crawl_id").toString());
            LOGGER.info("CDC for crawl job : " + crawl_id);


//cdc for table metadata
            if (runAttributes.get("table_metadata").toString().equals("Y")) {
                LOGGER.info("CDC for table_metadata started ");
                ST koshInsert_TableMetadataFromSource = SQLgroup_Kosh.getInstanceOf("koshInsert_TableMetadataFromSource");
                new ModakQueryRunner().update(connectionObj_kosh, koshInsert_TableMetadataFromSource.render());
            }
//cdc for column metadata
            if (runAttributes.get("columns_metadata").toString().equals("Y")) {
                LOGGER.info("CDC for columns_metadata started ");
                ST koshInsert_ColumnsDataFromSource = SQLgroup_Kosh.getInstanceOf("koshInsert_ColumnsDataFromSource");
                new ModakQueryRunner().update(connectionObj_kosh, koshInsert_ColumnsDataFromSource.render());
            }
//cdc for index metadata
            if (runAttributes.get("indexes_metadata").toString().equals("Y")) {
                LOGGER.info("CDC for indexes_metadata started ");
                ST koshInsert_IndexColumnsFromSource = SQLgroup_Kosh.getInstanceOf("koshInsert_IndexColumnsFromSource");
                new ModakQueryRunner().update(connectionObj_kosh, koshInsert_IndexColumnsFromSource.render());
            }
//cdc for table privileges metadata
            if (runAttributes.get("privileges_metadata").toString().equals("Y")) {
                LOGGER.info("CDC for privileges_metadata started ");
                ST koshInsert_TablePrivilegesFromSource = SQLgroup_Kosh.getInstanceOf("koshInsert_TablePrivilegesFromSource");
                new ModakQueryRunner().update(connectionObj_kosh, koshInsert_TablePrivilegesFromSource.render());
            }

            // Data_Spider_Verification_Batch_insertion

            try {
                LOGGER.info("Data_Spider_Verification_Batch_insertion has started");
                ST insert_into_data_spider_verification_batch = SQLgroup_Kosh
                        .getInstanceOf("insertIntoDataSpiderVerificationBatch");
                queryRunner.update(connectionObj_kosh, insert_into_data_spider_verification_batch.render());
            }
            catch (NullPointerException se) {
                se.printStackTrace();
            }

            long t2 = System.currentTimeMillis();
            LOGGER.debug("DataSpider CDC completed in : " + TimeUnit.MILLISECONDS.toMinutes((t2 - t1)) + " Minutes");

            // commit the CDC
            connectionObj_kosh.commit();
            connectionObj_kosh.setAutoCommit(false);
            ST updateCrawlJobInfo_Finished = SQLgroup_Kosh.getInstanceOf("updateCrawlJobInfo_Finished");
            updateCrawlJobInfo_Finished.add("crawl_id", crawl_id);
            // update the crawljobno in crawl jobinfo table
            queryRunner.update(connectionObj_kosh, updateCrawlJobInfo_Finished.render());
            LOGGER.info("DataSpider Crawl job info updated");
            connectionObj_kosh.commit();

        }
        catch (SQLException ex) {
            ex.printStackTrace();
            isErrored = 1;
            LOGGER.error("Error while performing CDC SQLs" + ex);

        }
        catch (Exception ex) {
            isErrored = 1;
            LOGGER.error("Error while performing CDC" + ex);
            ex.printStackTrace();
            LoggerUtils.insertLog(this.getClass().getSimpleName(), "Error while performing CDC " + ex, false);
        }
        finally {
            if (isErrored == 1) {
                if (connectionObj_kosh != null) {
                    try {
                        connectionObj_kosh.close();
                    }
                    catch (SQLException e) {
                        e.printStackTrace();
                        LOGGER.error("Error while closing the Kosh connection\n" + e);
                        LoggerUtils.insertLog(this.getClass().getSimpleName(),
                                "Error while closing the Kosh connection\n" + e, false);
                    }
                }
                LoggerUtils.insertLog(this.getClass().getSimpleName(), "Error while performing CDC", true);
            }
        }
    }
    /**
     * @param obj
     * @return the empty stirng ""  if passed object is null.
     */
    public Object emptyIfNull(Object obj) {
        if (obj == null) {
            return "";
        }
        else {
            return obj.toString();
        }
    }


    /**
     * main method for dataspider which will take 2 arguments: configuration file path & source , which need to be
     * crawled.
     *
     * @param args
     */
    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("h", "help", false, "show help.");
        options.addOption("r", "resourcepath", true,
            "Resources directory where dataSpider configurations are present such as database configurations, String template files");
        options.addOption("s", "sources", true,
            "Sources to be executed (Eg: oracle,sql_server or all_sources to crawl all sources)");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        // getting resource files path from arguments
        try {
            cmd = parser.parse(options, args);
            if (cmd.hasOption("h") || args.length == 0) {
                help(options);
            }
            if (cmd.hasOption("r") && cmd.hasOption("s")) {
                runningSource = cmd.getOptionValue("s");
                resources_path = cmd.getOptionValue("r");
                File file = new File(resources_path);
                if (!file.isDirectory()) {
                    help(options);
                }
            }
            else {
                help(options);
            }
        }
        catch (Exception e) {
            e.printStackTrace();

        }
        int isErrored = 0;
        // load the log4j2.xml properties file, if it doesn't find default
        // logger is used
        Configurator.initialize(null, resources_path + File.separator + "logging" + File.separator + "log4j2.xml");
        LOGGER.info("Using CLI options \n\t -r resource path : " + resources_path + "\n\t -s running source : "
            + runningSource);
        DataSpider crawler = new DataSpider();
        try {
            // iniatialise resource path
            LoggerUtils.initialiseResourcePath(resources_path);
            // create connection pool for Kosh logger
            ConnectionManagerKosh.configureHikariCPConnectionPool(resources_path);
            // initialize the Kosh Crawler
            crawler.init();
        }
        catch (Exception ex) {
            ex.printStackTrace();
            LOGGER.error(
                "Error while reading the logger config file = log4j2.xml at path supplied" + resources_path + "!");
            LoggerUtils.insertLog("KoshCrawler",
                "Error while reading the logger config file = log4j2.xml at path supplied" + resources_path + "!",
                false);
        }
        Connection connectionObj_kosh = null;
        try {
            // get the Kosh Connection object
            connectionObj_kosh = DriverManager.getConnection(crawler.koshAttributes.get("jdbc_url"),
                crawler.koshAttributes.get("username"), crawler.koshAttributes.get("passwd"));
            // check if connection obj is null and halt the crawler
            if (connectionObj_kosh == null) {
                LOGGER.error("Error while creating Kosh Connection object !");
                LoggerUtils.insertLog(crawler.getClass().getSimpleName(),
                    "Error while creating Kosh Connection object !", true);
            }

            //call rollBack process
            crawler.rollBack(connectionObj_kosh);
            // call the crawling process
            if(crawler.runAttributes.get("crawling").toString().equals("Y")) {
            crawler.doCrawling(connectionObj_kosh);}
            //call the cdc process
            if(crawler.runAttributes.get("cdc").toString().equals("Y")) {
                crawler.doCDC(connectionObj_kosh);}


        }
        catch (Exception ex) {
            isErrored = 1;
            ex.printStackTrace();
            LOGGER.error("Error while running Kosh Crawler !" + ex);
            LoggerUtils.insertLog(crawler.getClass().getSimpleName(), "Error while running Kosh Crawler !", false);
        }
        finally {
            if (isErrored == 1) {
                if (connectionObj_kosh != null) {
                    try {
                        //closing the Kosh connections
                        connectionObj_kosh.close();
                    }
                    catch (SQLException e) {
                        e.printStackTrace();
                        LOGGER.error("Error while closing the Kosh connection\n" + e);
                        LoggerUtils.insertLog(crawler.getClass().getSimpleName(),
                            "Error while closing the Kosh connection\n" + e, false);
                    }
                }
                System.exit(1);
            }
        }
    }

    //help for User

    private static void help(Options options) {
        HelpFormatter formater = new HelpFormatter();
        formater.printHelp("[dataSpider.jar] -r [resource directory] -s [sources to crawl]", options);
        System.exit(0);
    }
}
