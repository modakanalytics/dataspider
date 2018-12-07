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

import com.modak.dataspider.utils.JSONUtils;
import com.modak.dataspider.utils.RSAEncryptionUtils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * This class is an utility class to make the Connection pool up for Kosh repository
 *
 * @author modakanalytics
 * @version 1.0
 */
public class ConnectionManagerKosh {

    private static HikariDataSource hikariCPConnectionPool = null;
    private static final org.apache.logging.log4j.Logger LOGGER = LogManager.getLogger("ConnectionManagerKosh");

    public static HikariDataSource getConnectionPool() {
        return hikariCPConnectionPool;
    }

    public static void setConnectionPool(HikariDataSource connectionPool) {
        ConnectionManagerKosh.hikariCPConnectionPool = connectionPool;
    }

    //This method  is to make the Connection pool up for Kosh repository

    public static void configureHikariCPConnectionPool(String resources_path) {
        int isErrored = 0;
        try {
            Map<String, Object> connection_profile_map = JSONUtils.jsonToMap(FileUtils.readFileToString(
                new File(resources_path + File.separator + "connection_profiles" + File.separator + "kosh.json"),
                Charset.defaultCharset()));
            String password = connection_profile_map.get("passwd").toString();
            String privateKeyPath = resources_path + File.separator + "privateKey";
            if (connection_profile_map.containsKey("encrypted_passwd")
                && connection_profile_map.get("encrypted_passwd").toString().equals("Y")) {
                password = RSAEncryptionUtils.decryptPassword(password, privateKeyPath);

            }
            // setup the connection pool
            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setDriverClassName(connection_profile_map.get("jdbc_driver").toString());
            hikariConfig.setJdbcUrl(connection_profile_map.get("jdbc_url").toString());
            hikariConfig.setUsername(connection_profile_map.get("username").toString());
            hikariConfig.setPassword(password);
            hikariConfig.setMinimumIdle(
                Integer.parseInt(connection_profile_map.get("minConnectionsPerPartition").toString()));
            hikariConfig.setMaximumPoolSize(
                Integer.parseInt(connection_profile_map.get("maxConnectionsPerPartition").toString()));
            hikariConfig.addDataSourceProperty("cachePrepStmts", "true");
            hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
            hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
            hikariConfig.addDataSourceProperty("autoReconnect", true);
            hikariConfig.setConnectionTestQuery("select 1");
            hikariConfig.setLeakDetectionThreshold(10);
            hikariCPConnectionPool = new HikariDataSource(hikariConfig);
            setConnectionPool(hikariCPConnectionPool);
            LOGGER.info("Kosh Connection Pool is configured");

        }
        catch (Exception exe) {
            isErrored = 1;
            exe.printStackTrace();
            // log the error to console and file logs
            LOGGER.error("Error while Kosh Connection Pool is configured, so stopping the crawling Process" + exe);
        }
        finally {
            if (isErrored == 1) {
                LOGGER.error("Abrupt Exit !");
                System.exit(1);
            }
        }
    }

    public static void shutdownHikariCPConnectionPool() {
        try {
            HikariDataSource connectionPool = ConnectionManagerKosh.getConnectionPool();
            if (connectionPool != null) {
                // This method must be called only once when the application stops
                connectionPool.close();
                LOGGER.debug("Kosh Connection Pool is shut down!");
            }
        }
        catch (Exception exe) {
            exe.printStackTrace();
            LOGGER.error("Error while shutting down Kosh Connection Pool" + exe);
        }
    }
}