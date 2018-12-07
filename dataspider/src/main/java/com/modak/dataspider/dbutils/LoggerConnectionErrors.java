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

import org.apache.logging.log4j.LogManager;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STGroupFile;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;


/**
 * This class log the errors while connecting to Database
 *
 * @author modakanalytics
 * @version 1.0
 */

public class LoggerConnectionErrors {

    // logger object
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("LoggerConnectionErrors");

    // utility method for logging the error
    public static void insertLog(Connection koshConnection, long datastore_id, String connection_error_desc,
        long crawl_id, long prev_crawl_id, String resource_path) {
        STGroup SQLgroup = new STGroupFile(
            resource_path + File.separator + "templates" + File.separator + "kosh_sqls.stg", '$', '$');
        int isErrored = 0;
        try {
            // QueryRunner object used in running SQL queries
            ModakQueryRunner queryRunner = new ModakQueryRunner();
            ST insert_error_into_connection_error_table = SQLgroup
                .getInstanceOf("insert_error_into_connection_error");

            Object[] obj = {datastore_id, connection_error_desc, crawl_id, prev_crawl_id,
                new Date(System.currentTimeMillis()).toString()};
            queryRunner.update(koshConnection, insert_error_into_connection_error_table.render(), obj);

            logger.debug("Logged error into connection_error with error message : " + connection_error_desc);
        }
        catch (SQLException e) {
            isErrored = 1;
            e.printStackTrace();
            logger.error("Error logging error into connection_error : " + connection_error_desc
                + " Exception is: " + e);
        }
        finally {
            try {
                if (koshConnection != null) {
                    koshConnection.close();
                }
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
}
