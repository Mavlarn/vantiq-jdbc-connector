package io.vantiq.ext.jdbc.handler;

import io.vantiq.ext.jdbc.JDBC;
import io.vantiq.ext.jdbc.JDBCConnector;
import io.vantiq.ext.jdbc.JDBCConnectorConfig;
import io.vantiq.extjsdk.ExtensionServiceMessage;
import io.vantiq.extjsdk.Handler;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class ConfigHandler extends Handler<ExtensionServiceMessage> {

    static final Logger LOG = LoggerFactory.getLogger(ConfigHandler.class);

    private static final String CONFIG = "config";
    private static final String JDBC_CONFIG = "jdbcConfig";

    private JDBCConnector connector;


    String USERNAME = "username";
    String PASSWORD = "password";
    String DB_URL = "dbURL";
    String POLL_TIME = "pollTime";
    String POLL_QUERY = "pollQuery";
    String LOAD_TABLE = "loadTable";
    String LOAD_INTERVAL = "loadInterval";
    String LOAD_Size = "loadSize";


    public ConfigHandler(JDBCConnector connector) {
        this.connector = connector;
    }

    /**
     *
     * @param message   A message to be handled
     */
    @Override
    public void handleMessage(ExtensionServiceMessage configMessage) {
        LOG.info("Configuration for source:{}", configMessage.getSourceName());

        /**
         * previous:
         *     {
         *        "vantiq": {
         *           "packageRows": "true"
         *        },
         *        "jdbcConfig": {
         *           "general": {
         *              "username": "sqlUsername",
         *              "password": "sqlPassword",
         *              "dbURL": "jdbc:mysql://localhost/myDB?useSSL=false&serverTimezone=UTC"
         *              "pollTime": 3000
         *              "pollQuery": "SELECT * FROM myTable"
         *           }
         *        }
         *     }
         *
         * now:
         *     {
         *        "jdbcConfig": {
         *              "username": "sqlUsername",
         *              "password": "sqlPassword",
         *              "dbURL": "jdbc:mysql://localhost/myDB?useSSL=false&serverTimezone=UTC"
         *              "pollTime": 3000
         *              "pollQuery": "SELECT * FROM myTable"
         *
         *        }
         *     }
         */

        Map<String, Object> messageObject = (Map) configMessage.getObject();

        Map<String, Object> config = (Map<String, Object>)messageObject.get(CONFIG);

        // Obtain entire config from the message object
        if ( !(config.get(JDBC_CONFIG) instanceof Map)) {
            LOG.error("Configuration failed. No configuration suitable for JDBC Source.");
            failConfig();
            return;
        }
        boolean success = createDBConnection((Map<String, Object>)config.get(JDBC_CONFIG));
        if (!success) {
            failConfig();
            return;
        }
        LOG.debug("Setup complete");

    }


    /**
     * Attempts to create the JDBC Source based on the configuration document.
     * @param generalConfig     The general configuration for the JDBC Source
     * @param vantiq            The vantiq configuration for the JDBC Source
     * @return                  true if the JDBC source could be created, false otherwise
     */
    boolean createDBConnection(Map<String, Object> jdbcConfig) {

        JDBCConnectorConfig config = JDBCConnectorConfig.fromMap(jdbcConfig);


        if (config.getUsername() == null || config.getPassword() == null || config.getDbURL() == null) {
            LOG.error("Configuration failed. Invalid config:{}", config);
            return false;
        }

        // Initialize JDBC Source with config values
            if (connector.getJdbc() != null) {
                connector.getJdbc().close();
            }
            JDBC jdbc = new JDBC(config);
            connector.setJdbc(jdbc);

        // Create polling query if specified
        if (StringUtils.isNotBlank(config.getPollQuery())) {
            int pollTime = config.getPollTime();
            if (pollTime <= 0) {
                LOG.error("Poll time must be greater than 0.");
                return false;
            }
            String pollQuery = config.getPollQuery();
            TimerTask task = new TimerTask() {
                @Override
                public void run() {
                    connector.executePolling(pollQuery);
                }
            };
            // Create new Timer, and schedule the task according to the pollTime
            Timer pollTimer = new Timer("executePolling");
            pollTimer.schedule(task, 0, pollTime);
            connector.setScheduledTimer(pollTimer);

        } else if (StringUtils.isNotBlank(config.getLoadTable())) {
            Timer pollTimer = connector.executeLoading(config.getLoadTable(), config.getLoadInterval(), config.getLoadSize());
            connector.setScheduledTimer(pollTimer);
        }

        LOG.trace("JDBC source created");
        return true;
    }
    
    /**
     * Closes the source {@link SFTPConnector} and marks the configuration as completed. The source will
     * be reactivated when the source reconnects, due either to a Reconnect message (likely created by an update to the
     * configuration document) or to the WebSocket connection crashing momentarily.
     */
    private void failConfig() {
        connector.close();
    }

}
