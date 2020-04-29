package io.vantiq.ext.jdbc.handler;

import io.vantiq.ext.jdbc.JDBCConnector;
import io.vantiq.extjsdk.ExtensionServiceMessage;
import io.vantiq.extjsdk.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.RejectedExecutionException;


public class PublishHandler extends Handler<ExtensionServiceMessage> {

    static final Logger LOG = LoggerFactory.getLogger(PublishHandler.class);

    private JDBCConnector connector;

    public PublishHandler(JDBCConnector connector) {
        this.connector = connector;
    }

    @Override
    public void handleMessage(ExtensionServiceMessage message) {
        LOG.debug("Publish with message " + message.toString());

        try {
            connector.executePublish(message);
        } catch (RejectedExecutionException e) {
            LOG.error("The queue of tasks has filled, and as a result the request was unable to be processed.", e);
        }

    }

}
