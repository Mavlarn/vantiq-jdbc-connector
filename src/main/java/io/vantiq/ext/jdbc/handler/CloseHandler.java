package io.vantiq.ext.jdbc.handler;

import io.vantiq.ext.jdbc.JDBCConnector;
import io.vantiq.extjsdk.ConnectorConfig;
import io.vantiq.extjsdk.ExtensionWebSocketClient;
import io.vantiq.extjsdk.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.vantiq.extjsdk.ConnectorConstants.CONNECTOR_CONNECT_TIMEOUT;
import static io.vantiq.extjsdk.ConnectorConstants.RECONNECT_INTERVAL;

public class CloseHandler extends Handler<ExtensionWebSocketClient> {

    static final Logger LOG = LoggerFactory.getLogger(CloseHandler.class);

    private JDBCConnector connector;

    public CloseHandler(JDBCConnector connector) {
        this.connector = connector;
    }

    @Override
    public void handleMessage(ExtensionWebSocketClient client) {

        LOG.info("Close handler: {}", client);

        if (connector.getScheduledTimer() != null) {
            connector.getScheduledTimer().cancel();
            connector.setScheduledTimer(null);
        }

        boolean sourcesSucceeded = false;
        while (!sourcesSucceeded) {
            ConnectorConfig config = connector.getConnectionInfo();
            client.initiateFullConnection(config.getVantiqUrl(), config.getToken());
            sourcesSucceeded = connector.checkConnectionFails(client, CONNECTOR_CONNECT_TIMEOUT);
            if (!sourcesSucceeded) {
                try {
                    Thread.sleep(RECONNECT_INTERVAL);
                } catch (InterruptedException e) {
                    LOG.error("An error occurred when trying to sleep the current thread. Error Message: ", e);
                }
            }
        }


    }
}
