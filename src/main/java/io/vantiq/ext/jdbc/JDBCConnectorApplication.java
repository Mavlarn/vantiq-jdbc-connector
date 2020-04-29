package io.vantiq.ext.jdbc;

public class JDBCConnectorApplication {

    public static void main(String[] args) {

        JDBCConnector connector = new JDBCConnector();
        connector.start();
    }

}
