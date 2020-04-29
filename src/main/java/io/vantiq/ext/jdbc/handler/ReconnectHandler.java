package io.vantiq.ext.jdbc.handler;

import io.vantiq.ext.jdbc.JDBCConnector;
import io.vantiq.extjsdk.ExtensionServiceMessage;
import io.vantiq.extjsdk.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ReconnectHandler extends Handler<ExtensionServiceMessage> {

    static final Logger LOG = LoggerFactory.getLogger(ReconnectHandler.class);

    private JDBCConnector connector;

    public ReconnectHandler(JDBCConnector connector) {
        this.connector = connector;
    }

    @Override
    public void handleMessage(ExtensionServiceMessage message) {

        LOG.trace("Reconnect message received. Reinitializing configuration");

        if (connector.getScheduledTimer() != null) {
            connector.getScheduledTimer().cancel();
            connector.setScheduledTimer(null);
        }

        CompletableFuture<Boolean> success = connector.getVantiqClient().connectToSource();

        try {
            if ( !success.get(10, TimeUnit.SECONDS) ) {
                if (!connector.getVantiqClient().isOpen()) {
                    LOG.error("Failed to connect to server url '" + connector.getConnectionInfo().getVantiqUrl() + "'.");
                } else if (!connector.getVantiqClient().isAuthed()) {
                    LOG.error("Failed to authenticate within 10 seconds using the given authentication data.");
                } else {
                    LOG.error("Failed to connect within 10 seconds");
                }
                connector.close();
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOG.error("Could not reconnect to source within 10 seconds: ", e);
            connector.close();
        }
    }
}
