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

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;

/**
 *
 * This class provide the hikari connection pool for the application
 * @author modakanalytics
 * @version 1.0
 */
public class ConnectionPoolUtils {
	private static final org.apache.logging.log4j.Logger LOGGER = LogManager.getLogger("ShutDownConnectionPools");

	// Shutting down of Kosh Connection pool without Exit of Kosh Crawling Process
	public static void shutDownConnectionPools() {
		try {
			ConnectionManagerKosh.shutdownHikariCPConnectionPool();
			LOGGER.info("Shutting down of Kosh Connection pool was successful !");
		} catch (Exception ex) {
			LOGGER.error("Error in shutting down Kosh Connection pool !");
		}
	}
		// Shutting down of Kosh Connection pool was successful with Exit of Kosh Crawling Process
	public static void shutDownConnectionPoolsWithExit() {
		try {
			ConnectionManagerKosh.shutdownHikariCPConnectionPool();
			LOGGER.info("Shutting down of Kosh Connection pool was successful with Exit of Kosh Crawling Process !");
		} catch (Exception ex) {
			LOGGER.error("Error in shutting down Kosh Connection pool !");
		} finally {
			LOGGER.error("Abrupt Exit !");
			System.exit(1);
		}
	}
		//closing the connection.
	public static void closeConnections(Connection koshConnection, Connection hiveMetaStoreConnection) {
		if (koshConnection != null) {
			try {
				koshConnection.close();
			} catch (SQLException e) {
				e.printStackTrace();
				LOGGER.error("Error while closing kosh connection" + e);
			}
		}
	}
}
