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

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * utility class for execution of sql query based on  different use case and parameters
 * @author modakanalytics
 *
 * @version 1.0
 */
public class ModakQueryRunner extends QueryRunner {

    public <T> T query(Connection conn, String sql, ResultSetHandler<T> rsh, boolean readOnly) throws SQLException {
        return this.<T>query(conn, false, sql, rsh, readOnly, (Object[]) null);
    }

    public <T> T query(Connection conn, String sql, ResultSetHandler<T> rsh, boolean readOnly, Object... params)
        throws SQLException {
        return this.<T>query(conn, false, sql, rsh, readOnly, params);
    }

    private <T> T query(Connection conn, boolean closeConn, String sql, ResultSetHandler<T> rsh, boolean readOnly,
        Object... params) throws SQLException {
        if (conn == null) {
            throw new SQLException("Null connection");
        }

        if (sql == null) {
            if (closeConn) {
                close(conn);
            }
            throw new SQLException("Null SQL statement");
        }

        if (rsh == null) {
            if (closeConn) {
                close(conn);
            }
            throw new SQLException("Null ResultSetHandler");
        }

        PreparedStatement stmt = null;
        ResultSet rs = null;
        T result = null;
        boolean commit = conn.getAutoCommit();
        try {
            if (commit) {
                conn.setAutoCommit(false);
            }
            stmt = this.prepareStatement(conn, sql, readOnly);

            this.fillStatement(stmt, params);
            rs = this.wrap(stmt.executeQuery());
            result = rsh.handle(rs);

        }
        catch (SQLException e) {
            this.rethrow(e, sql, params);

        }
        finally {
            try {
                close(rs);
                conn.setAutoCommit(commit);
            }
            finally {
                close(stmt);
                if (closeConn) {
                    close(conn);
                }
            }
        }

        return result;
    }

    //this method initiate the preparestament and execute the sql query
    protected PreparedStatement prepareStatement(Connection conn, String sql, boolean readOnly) throws SQLException {
        if (readOnly) {
            PreparedStatement stmt = conn
                .prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(10000);
            return stmt;

        }
        else {
            return conn.prepareStatement(sql);
        }
    }
}
