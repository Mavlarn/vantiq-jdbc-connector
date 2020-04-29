package io.vantiq.ext.jdbc.handler;

import io.vantiq.ext.jdbc.JDBCConnector;
import io.vantiq.extjsdk.ExtensionServiceMessage;
import io.vantiq.extjsdk.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.RejectedExecutionException;

public class QueryHandler extends Handler<ExtensionServiceMessage> {

    static final Logger LOG = LoggerFactory.getLogger(QueryHandler.class);

    private JDBCConnector connector;

    public QueryHandler(JDBCConnector connector) {
        this.connector = connector;
    }

    @Override
    public void handleMessage(ExtensionServiceMessage message) {
        LOG.debug("query");

        try {
            connector.executeQuery(message);
        } catch (RejectedExecutionException e) {
            LOG.error("The queue of tasks has filled, and as a result the request was unable to be processed.", e);
            String replyAddress = ExtensionServiceMessage.extractReplyAddress(message);
            connector.getVantiqClient().sendQueryError(replyAddress, "io.vantiq.extsrc.JDBCHandleConfiguration.queryHandler.queuedTasksFull",
                    "The queue of tasks has filled, and as a result the request was unable to be processed.", null);
        }
    }
}
