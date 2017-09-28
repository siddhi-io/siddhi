/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.extension.eventtable.rdbms;

import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBConnectionHelper {

    private Logger log = Logger.getLogger(DBConnectionHelper.class);
    private static DBConnectionHelper dbConnectionHelper;

    private DBConnectionHelper() {

    }

    public static DBConnectionHelper getDBConnectionHelperInstance() {
        if (dbConnectionHelper == null) {
            dbConnectionHelper = new DBConnectionHelper();
        }
        return dbConnectionHelper;
    }

    public void clearDatabaseTable(DataSource dataSource,String tableName) {
        PreparedStatement stmt = null;
        Connection con = null;
        try {
            con = dataSource.getConnection();
            con.setAutoCommit(false);
            stmt = con.prepareStatement("DELETE IGNORE FROM " + tableName + "");
            stmt.executeUpdate();
            con.commit();

        } catch (SQLException e) {
            log.error("Error while deleting the event", e);
        } finally {
            clearConnections(stmt, con);
        }
    }

    public void createTestDatabaseTable(DataSource dataSource, String tableName) {
        PreparedStatement stmt = null;
        Connection con = null;
        try {
            con = dataSource.getConnection();
            con.setAutoCommit(false);
            stmt = con.prepareStatement("CREATE TABLE " + tableName + "( " +
                    "SYMBOL varchar(10) PRIMARY KEY," +
                    "PRICE float," +
                    "VOLUME bigint(20)" +
                    ");");
            stmt.executeUpdate();
            con.commit();

        } catch (SQLException e) {
            log.error("Error while creating the table", e);
        } finally {
            clearConnections(stmt, con);
        }
    }

    public void createTestDatabaseTableWithSchema(DataSource dataSource, String schema) {
        PreparedStatement stmt = null;
        Connection con = null;
        try {
            con = dataSource.getConnection();
            con.setAutoCommit(false);
            stmt = con.prepareStatement(schema);
            stmt.executeUpdate();
            con.commit();

        } catch (SQLException e) {
            log.error("Error while creating the table", e);
        } finally {
            clearConnections(stmt, con);
        }
    }

    public boolean isTableExist(DataSource dataSource, String tableName) {
        PreparedStatement stmt = null;
        Connection con = null;
        try {
            con = dataSource.getConnection();
            con.setAutoCommit(false);
            stmt = con.prepareStatement("SELECT 1 FROM " +
                    tableName +
                    " LIMIT 1;");
            con.commit();
            return stmt.execute();
        } catch (SQLException e) {
            log.error("Error while creating the event", e);
            return false;
        } finally {
            clearConnections(stmt, con);
        }
    }

    public void insertTestDataIntoTable(DataSource dataSource) {
        PreparedStatement stmt = null;
        Connection con = null;
        try {
            con = dataSource.getConnection();
            con.setAutoCommit(false);
            stmt = con.prepareStatement("" +
                    "INSERT INTO " + RDBMSTestConstants.TABLE_NAME +
                    " (symbol,price,volume) " +
                    "VALUES('WSO2',2.0,3),('IBM',3.0,4),('GOOGLE',5.0,6)");
            stmt.executeUpdate();
            con.commit();

        } catch (SQLException e) {
            log.error("Error while insert test data into table", e);
        } finally {
            clearConnections(stmt, con);
        }
    }

    public void deleteTable(DataSource dataSource, String tableName) {
        PreparedStatement stmt = null;
        Connection con = null;
        try {
            con = dataSource.getConnection();
            con.setAutoCommit(false);
            stmt = con.prepareStatement("" +
                    "DROP TABLE " + tableName + ";");
            stmt.executeUpdate();
            con.commit();

        } catch (SQLException e) {
            log.error("Error while deleting table", e);
        } finally {
            clearConnections(stmt, con);
        }
    }

    public void insertTestDataIntoTableWithQuery(DataSource dataSource, String query) {
        PreparedStatement stmt = null;
        Connection con = null;
        try {
            con = dataSource.getConnection();
            con.setAutoCommit(false);
            stmt = con.prepareStatement(query);
            stmt.executeUpdate();
            con.commit();

        } catch (SQLException e) {
            log.error("Error while insert test data into table", e);
        } finally {
            clearConnections(stmt, con);
        }
    }

    public long getRowsInTable(DataSource dataSource) {

        PreparedStatement stmt = null;
        Connection con = null;
        long count = 0;

        try {
            con = dataSource.getConnection();
            stmt = con.prepareStatement("SELECT * FROM " + RDBMSTestConstants.TABLE_NAME + "");
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                count++;
            }
        } catch (SQLException e) {
            clearConnections(stmt, con);
        }
        return count;

    }

    public long getRowsInTable(DataSource dataSource, String tableName) {

        PreparedStatement stmt = null;
        Connection con = null;
        long count = 0;

        try {
            con = dataSource.getConnection();
            stmt = con.prepareStatement("SELECT * FROM " + tableName + "");
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                count++;
            }
        } catch (SQLException e) {
            clearConnections(stmt, con);
        }
        return count;

    }

    public void clearConnections(PreparedStatement stmt, Connection con) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                log.error("unable to release statement", e);
            }
        }

        if (con != null) {
            try {
                con.close();
            } catch (SQLException e) {
                log.error("unable to release connection", e);
            }
        }

    }

}
