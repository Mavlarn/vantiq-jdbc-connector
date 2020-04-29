package io.vantiq.ext.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

public class JDBCConnectorConfig {

    String username;
    String password;
    String dbURL;
    int pollTime = 1000; // default 1000 ms
    String pollQuery;
    int poolSize = 10; // default pool size 10

    String loadTable;
    int loadInterval;
    int loadSize;

    public JDBCConnectorConfig() { }

    public static JDBCConnectorConfig fromMap(Map<String, Object> sourceConfig) {
        ObjectMapper mapper = new ObjectMapper();
        JDBCConnectorConfig config = mapper.convertValue(sourceConfig, JDBCConnectorConfig.class);
        return config;
    }

    public String getUsername() {

        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDbURL() {
        return dbURL;
    }

    public int getPollTime() {
        return pollTime;
    }

    public String getPollQuery() {
        return pollQuery;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public String getLoadTable() {
        return loadTable;
    }

    public int getLoadInterval() {
        return loadInterval;
    }

    public int getLoadSize() {
        return loadSize;
    }

    @Override
    public String toString() {
        return "JDBCConnectorConfig{" +
                "username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", dbURL='" + dbURL + '\'' +
                ", pollTime=" + pollTime +
                ", pollQuery='" + pollQuery + '\'' +
                ", poolSize=" + poolSize +
                ", loadTable='" + loadTable + '\'' +
                ", loadInterval=" + loadInterval +
                ", loadSize=" + loadSize +
                '}';
    }
}
