/*
 * Copyright (c) 2018 Vantiq, Inc.
 *
 * All rights reserved.
 * 
 * SPDX: MIT
 */

package io.vantiq.ext.jdbc;

import cn.ffcs.memory.Memory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JDBC {

    private static final Logger LOG  = LoggerFactory.getLogger(JDBC.class);
    private Connection  conn = null;

    // Boolean flag specifying if publish/query requests are handled synchronously, or asynchronously
//    boolean isAsync;
    
    // Used to reconnect if necessary
    private String dbURL;
    private String username;
    private String password;
    
    // Timeout (in seconds) used to check if connection is still valid
    private static final int CHECK_CONNECTION_TIMEOUT = 5;

    // Timeout (in milliseconds) specifying how long ds.getConnection() will wait for a connection before timing out
    private static final int CONNECTION_POOL_TIMEOUT = 5000;

    // Used if asynchronous publish/query handling has been specified
    private HikariDataSource ds;
    private Memory memory;

    DateFormat dfTimestamp  = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    DateFormat dfDate       = new SimpleDateFormat("yyyy-MM-dd");
    DateFormat dfTime       = new SimpleDateFormat("HH:mm:ss.SSSZ");

    /**
     * The method used to setup the connection to the SQL Database, using the values retrieved from the source config.
     */
    public JDBC(JDBCConnectorConfig config) {

        // Save login credentials for reconnection if necessary
        this.dbURL = config.getDbURL();
        this.username = config.getUsername();
        this.password = config.getPassword();

        // Create a connection pool
        HikariConfig connectionPoolConfig = new HikariConfig();
        connectionPoolConfig.setJdbcUrl(dbURL);
        if (username != null) {
            connectionPoolConfig.setUsername(username);
        }
        if (password != null) {
            connectionPoolConfig.setPassword(password);
        }
        ds = new HikariDataSource(connectionPoolConfig);
        ds.setConnectionTimeout(CONNECTION_POOL_TIMEOUT);

        // Setting max pool size (should always match number of active threads for publish and query)
        ds.setMaximumPoolSize(config.getPoolSize());

        memory = new Memory(ds);
    }
    
    /**
     * The method used to execute the provided query, triggered by a SELECT on the respective source from VANTIQ.
     * @param sqlQuery          A String representation of the query, retrieved from the WITH clause from VANTIQ.
     * @return                  A HashMap Array containing all of the data retrieved by the query, (empty HashMap 
     *                          Array if nothing was returned)
     * @throws VantiqSQLException
     */
    public HashMap[] processQuery(String sqlQuery) throws VantiqSQLException {
        HashMap[] rsArray = null;

        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sqlQuery)) {
            rsArray = createMapFromResults(rs);
        } catch (SQLException e) {
            // Handle errors for JDBC
            reportSQLError(e);
        }

        return rsArray;
    }
    
    /**
     * The method used to execute the provided query, triggered by a PUBLISH on the respective VANTIQ source.
     * @param sqlQuery          A String representation of the query, retrieved from the PUBLISH message.
     * @return                  The integer value that is returned by the executeUpdate() method representing the row count.
     * @throws VantiqSQLException
     */
    public int processPublish(String sqlQuery) throws VantiqSQLException {
        int publishSuccess = -1;

        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            publishSuccess = stmt.executeUpdate(sqlQuery);
        } catch (SQLException e) {
            // Handle errors for JDBC
            reportSQLError(e);
        }

        return publishSuccess;
    }

    public int processInsert(String table, Map data) {
        return memory.create(table, data);
    }

    /**
     * The method used to execute the provided list of queries, triggered by a PUBLISH on the respective VANTIQ source. These queries
     * are processed as a batch.
     * @param queryList             The list of queries to be processed as a batch.
     * @return
     * @throws VantiqSQLException
     * @throws ClassCastException
     */
    public int[] processBatchPublish(List queryList) throws VantiqSQLException, ClassCastException {
        int[] publishSuccess = null;

        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {

            // Adding queries into batch
            for (int i = 0; i < queryList.size(); i++) {
                stmt.addBatch((String) queryList.get(i));
            }

            // Executing the batch
            publishSuccess = stmt.executeBatch();
        } catch (SQLException e) {
            // Handle errors for JDBC
            reportSQLError(e);
        }

        return publishSuccess;
    }
    
    /**
     * Method used to create a map out of the output ResultSet. Map is needed in order to send the data back to VANTIQ
     * @param queryResults   A ResultSet containing return value from executeQuery()
     * @return               A HashMap Array containing all of the rows from the ResultSet, each converted to a HashMap,
     *                       (or an empty HashMap Array if the ResultSet was empty).
     * @throws VantiqSQLException
     */
    HashMap[] createMapFromResults(ResultSet queryResults) throws VantiqSQLException {
        ArrayList<HashMap> rows = new ArrayList<HashMap>();
        try {
            if (!queryResults.next()) { 
                return rows.toArray(new HashMap[rows.size()]);
            } else {
                ResultSetMetaData md = queryResults.getMetaData(); 
                int columns = md.getColumnCount();
                
                // Iterate over rows of Result Set and create a map for each row
                do {
                    HashMap row = new HashMap(columns);
                    for (int i=1; i<=columns; ++i) {
                        // Check column type to retrieve data in appropriate manner
                        int columnType = md.getColumnType(i);
                        switch (columnType) {
                            case Types.DECIMAL:
                                if (queryResults.getBigDecimal(i) != null) {
                                    row.put(md.getColumnName(i), queryResults.getBigDecimal(i));
                                }
                                break;
                            case Types.DATE:
                                Date rowDate = queryResults.getDate(i);
                                if (rowDate != null) {
                                    row.put(md.getColumnName(i), dfDate.format(rowDate));
                                }
                                break;
                            case Types.TIME:
                                Time rowTime = queryResults.getTime(i);
                                if (rowTime != null) {
                                    row.put(md.getColumnName(i), dfTime.format(rowTime));
                                }
                                break;
                            case Types.TIMESTAMP:
                                Timestamp rowTimestamp = queryResults.getTimestamp(i);
                                if (rowTimestamp != null) {
                                    row.put(md.getColumnName(i), dfTimestamp.format(rowTimestamp));
                                }
                                break;
                            default:
                                // If none of the initial cases are met, the data will be converted to a String via getObject()
                                if(queryResults.getObject(i) != null) {
                                    row.put(md.getColumnName(i), queryResults.getObject(i));
                                }
                                break;
                        }
                    }
                    // Add each row map to the list of rows
                    rows.add(row);
                } while(queryResults.next());
            }
        } catch (SQLException e) {
            reportSQLError(e);
        }
        HashMap[] rowsArray = rows.toArray(new HashMap[rows.size()]);
        return rowsArray;
    }
    
    /**
     * Method used to try and reconnect if database connection was lost. Used for synchronous processing (connection pool handles this internally).
     * @throws VantiqSQLException
     */
    public void diagnoseConnection() throws VantiqSQLException {
        try {
            if (!conn.isValid(CHECK_CONNECTION_TIMEOUT)) {
                conn = DriverManager.getConnection(dbURL,username,password);
            }
        } catch (SQLException e) {
            // Handle errors for JDBC
            reportSQLError(e);
        }
    }
    
    /**
     * Method used to throw the VantiqSQLException whenever is necessary
     * @param e The SQLException caught by the calling method
     * @throws VantiqSQLException
     */
    public void reportSQLError(SQLException e) throws VantiqSQLException {
        String message = this.getClass().getCanonicalName() + ": A database error occurred: " + e.getMessage() +
                " SQL State: " + e.getSQLState() + ", Error Code: " + e.getErrorCode();
        throw new VantiqSQLException(message);
    }

    public DataSource getDataSource() {
        return ds;
    }

    /**
     * Closes the SQL Connection.
     */
    public void close() {
        // Close single connection if open
        try {
            if (conn!=null) {
                conn.close();
            }
        } catch(SQLException e) {
            LOG.error("A error occurred when closing the Connection: ", e);
        }
        // Close connection pool if open
        if (ds != null) {
            ds.close();
        }
    }
}

