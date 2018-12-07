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
package com.modak.dataspider.common;

/**
 * This class contains all the string constants used in the dataspider project
 */
public class DataSpiderCommon {

    // JSON files
    public static final String KOSH_JSON = "kosh.json";
    public static final String RUNATTRIBUTES_JSON = "runAttributes.json";

    // STG files
    public static final String KOSH_SQLS_STG = "kosh_sqls.stg";
    public static final String ROLLBACK_SQLS_STG = "roll_back_sqls.stg";

    // JSON keys for configuration
    public static final String JSON_CONFIG_ENCRYPTED_PASSWORD = "encrypted_passwd";
    public static final String JSON_CONFIG_PASSWORD = "passwd";
    public static final String JSON_CONFIG_JDBC_DRIVER = "jdbc_driver";
    public static final String JSON_CONFIG_JDBC_URL = "jdbc_url";
    public static final String JSON_CONFIG_USERNAME = "username";
    public static final String JSON_CONFIG_THREADS = "threads";
    public static final String JSON_CONFIG_MIN_CONNECTIONS_PER_PARTITION = "minConnectionsPerPartition";
    public static final String JSON_CONFIG_MAX_CONNECTIONS_PER_PARTITION = "maxConnectionsPerPartition";
    public static final String JSON_CONFIG_CACHE_PREP_STMTS = "cachePrepStmts";
    public static final String JSON_CONFIG_PREP_STMT_CACHE_SIZE = "prepStmtCacheSize";
    public static final String JSON_CONFIG_PREP_STMT_CACHE_SQL_LIMIT = "prepStmtCacheSqlLimit";
    public static final String JSON_CONFIG_AUTORECONNECT = "autoReconnect";

    public static final String POSTGRESQL_TEST_QUERY = "select 1";

    //flag
    public static final String FLAG_Y = "Y";

    //misc
    public static final String CRAWL_ID = "crawl_id";
    public static final String RESOURCES_PATH = "resources_path";


}
