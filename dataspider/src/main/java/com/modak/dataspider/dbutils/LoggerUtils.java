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
package com.modak.dataspider.dbutils;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STGroupFile;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * initialize the log and insert the error in lob table
 *
 * @author modakanalytics
 * @version 1.0
 */

public class LoggerUtils {

    static String resources_path = null;

    // logger object
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("LoggerUtils");

    // utility method for logging the error

    public static void initialiseResourcePath(String resources_path) {
        LoggerUtils.resources_path = resources_path;
        logger.info("Logger resource path" + resources_path);

    }

    /**
     * it will insert the error message into log table
     *
     * @param log_message_type               type of log message
     * @param message                        error message
     * @param connectionPoolShutdownWithExit
     */
    public static void insertLog(String log_message_type, String message, boolean connectionPoolShutdownWithExit) {
        // SQL String Template group
        STGroup SQLgroup = new STGroupFile(
            resources_path + File.separator + "templates" + File.separator + "kosh_sqls.stg", '$', '$');
        // get Kosh Connection object from Connection pool
        HikariDataSource connectionPoolKosh = ConnectionManagerKosh.getConnectionPool();
        Connection koshConnection = null;
        int isErrored = 0;
        try {
            koshConnection = connectionPoolKosh.getConnection();

        }
        catch (SQLException e) {
            isErrored = 1;
            e.printStackTrace();
            logger.error(
                "While logging error into log_table there is an Error in getting Connection object for Kosh Repository with log_message_type="
                    + log_message_type + " and error message =" + message + " Exception is: " + e);
            ConnectionPoolUtils.shutDownConnectionPoolsWithExit();
        }
        try {
            // QueryRunner object used in running SQL queries
            ModakQueryRunner queryRunner = new ModakQueryRunner();
            ST insert_error_into_log_table = SQLgroup.getInstanceOf("insert_error_into_log_table");
            Object[] insert_error_into_log_table_params = new Object[]{log_message_type, message};
            queryRunner
                .update(koshConnection, insert_error_into_log_table.render(), insert_error_into_log_table_params);
            logger.debug("Logged error into log_table with log_message_type=" + log_message_type
                + " and error message =" + message);
        }
        catch (Exception e) {
            isErrored = 1;
            e.printStackTrace();
            logger.error(
                "While logging error into log_table there is an Error in getting Connection object for Kosh Repository with log_message_type="
                    + log_message_type + " and error message =" + message + " Exception is: " + e);
        }
        finally {
            //closing the open connection
            if (koshConnection != null) {
                try {
                    koshConnection.close();
                }
                catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (isErrored == 1 || connectionPoolShutdownWithExit) {
                ConnectionPoolUtils.shutDownConnectionPoolsWithExit();
            }
        }
    }
}
