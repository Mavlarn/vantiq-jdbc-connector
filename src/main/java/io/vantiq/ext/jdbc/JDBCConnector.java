package io.vantiq.ext.jdbc;

import cn.ffcs.memory.JSONArrayHandler;
import cn.ffcs.memory.Memory;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.vantiq.ext.jdbc.handler.*;
import io.vantiq.extjsdk.ConnectorConfig;
import io.vantiq.extjsdk.ExtensionServiceMessage;
import io.vantiq.extjsdk.ExtensionWebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.vantiq.extjsdk.ConnectorConstants.CONNECTOR_CONNECT_TIMEOUT;
import static io.vantiq.extjsdk.ConnectorConstants.RECONNECT_INTERVAL;


public class JDBCConnector implements Closeable {

    static final Logger LOG = LoggerFactory.getLogger(JDBCConnector.class);

    private ExtensionWebSocketClient vantiqClient = null;
    private Timer scheduledTimer = null;
    private JDBC jdbc    = null;
    private ConnectorConfig connectionInfo;

    private final int DEFAULT_BUNDLE_SIZE = 500;
    private final String SYNCH_LOCK = "synchLock";


    public JDBCConnector() { }

    public void start() {
        connectionInfo = new ConnectorConfig();
        if (connectionInfo == null) {
            throw new RuntimeException("No VANTIQ connection information provided");
        }
        if (connectionInfo.getSourceName() == null) {
            throw new RuntimeException("No source name provided");
        }

        vantiqClient = new ExtensionWebSocketClient(connectionInfo.getSourceName());

        vantiqClient.setConfigHandler(new ConfigHandler(this));
        vantiqClient.setReconnectHandler(new ReconnectHandler(this));
        vantiqClient.setCloseHandler(new CloseHandler(this));
        vantiqClient.setPublishHandler(new PublishHandler(this));
        vantiqClient.setQueryHandler(new QueryHandler(this));

        boolean sourcesSucceeded = false;
        while (!sourcesSucceeded) {
            vantiqClient.initiateFullConnection(connectionInfo.getVantiqUrl(), connectionInfo.getToken());

            sourcesSucceeded = checkConnectionFails(vantiqClient, CONNECTOR_CONNECT_TIMEOUT);
            if (!sourcesSucceeded) {
                try {
                    Thread.sleep(RECONNECT_INTERVAL);
                } catch (InterruptedException e) {
                    LOG.error("An error occurred when trying to sleep the current thread. Error Message: ", e);
                }
            }
        }
    }

    /**
     * Executes the query that is provided as a String in the options specified by the "query" key, as part of the
     * object of the Query message. Calls sendDataFromQuery() if the query is executed successfully, otherwise sends
     * a query error using sendQueryError()
     * @param message   The Query message.
     */
    public void executeQuery(ExtensionServiceMessage message) {
        Map<String, ?> request = (Map<String, ?>) message.getObject();
        String replyAddress = ExtensionServiceMessage.extractReplyAddress(message);

        // Getting local copy of JDBC class
        JDBC localJDBC = null;
        synchronized (SYNCH_LOCK) {
            localJDBC = jdbc;
        }
        if (localJDBC == null) {
            if (vantiqClient != null) {
                vantiqClient.sendQueryError(replyAddress, this.getClass().getName() + ".closed",
                        "JDBC connection closed before operation could complete.", null);
            }
        }

        // Gather query results and send the appropriate response, or send a query error if an exception is caught
        try {
            if (request.get("query") instanceof String) {
                String queryString = (String) request.get("query");
                HashMap[] queryArray = localJDBC.processQuery(queryString);
                sendDataFromQuery(queryArray, message);
            } else {
                LOG.error("Query could not be executed because query was not a String.");
                vantiqClient.sendQueryError(replyAddress, this.getClass().getName() + ".queryNotString",
                        "The Publish Request could not be executed because the query property is"
                                + "not a string.", null);
            }
        } catch (Exception e) {
            LOG.error("An unexpected error occurred when executing the requested query.", e);
            LOG.error("Request was: {}", request);
            vantiqClient.sendQueryError(replyAddress, Exception.class.getCanonicalName(),
                    "Failed to execute query for reason: " + e.getMessage() +
                            ". Exception was: " + e.getClass().getName() + ". Request was: " + request.get("query"), null);
        }
    }

    /**
     * Executes the query that is provided in the Publish Message. If query is an Array of Strings, then it is executed as a Batch request.
     * If the query is a single String, then it is executed normally.
     * @param message   The Query message.
     */
    public void executePublish(ExtensionServiceMessage message) {
        Map<String, ?> request = (Map<String, ?>) message.getObject();

        // Gather query results, or send a query error if an exception is caught
        try {
            if (request.containsKey("table") && request.containsKey("data")) {
                String table = (String)request.get("table");
                Map data = (Map)request.get("data");
                int result = jdbc.processInsert(table, data);
            } else if (request.get("query") instanceof String) {
                String queryString = (String) request.get("query");
                int data = jdbc.processPublish(queryString);
                LOG.trace("The returned integer value from Publish Query is the following: ", data);
            } else if (request.get("query") instanceof List) {
                List queryArray = (List) request.get("query");
                int[] data = jdbc.processBatchPublish(queryArray);
                LOG.trace("The returned integer array from Publish Query is the following: ", data);
            } else {
                LOG.error("Query could not be executed because query was not a String or a List");
            }
        } catch (ClassCastException e) {
            LOG.error("Could not execute requested query. This is most likely because the query list did not contain Strings.", e);
            LOG.error("Request was: {}", request);
        } catch (Exception e) {
            LOG.error("An unexpected error occurred when executing the requested query.", e);
            LOG.error("Request was: {}", request);
        }
    }

    /**
     * Executes a query (pollQuery) at a certain rate (pollTime), both specified in the Source Configuration.
     * The resulting data is sent as a notification back to the Source. If multiple rows of data are returned,
     * then each row is sent as a separate notification.
     * @param pollQuery     The query string
     */
    public void executePolling(String pollQuery) {
        // Getting local copy of JDBC class
        JDBC localJDBC = null;
        synchronized (SYNCH_LOCK) {
            localJDBC = jdbc;
        }

        if (localJDBC == null) {
            return;
        }
        try {
            HashMap[] queryMap = localJDBC.processQuery(pollQuery);
            if (queryMap != null) {
                for (HashMap h : queryMap) {
                    vantiqClient.sendNotification(h);
                }
            }
        } catch (Exception e) {
            LOG.error("An unexpected error occurred when executing the polling query.", e);
            LOG.error("The pollQuery was: " + pollQuery);
        }
    }

    public Timer executeLoading(String loadTable, int loadInterval, int loadSize) {
        Memory memory = new Memory(this.jdbc.getDataSource());
        final int[] pageNo = {1}; // pagrNo start from 1
        JSONArrayHandler jsonArrayHandler = new JSONArrayHandler(false);
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                StringBuffer query = new StringBuffer("SELECT * FROM " + loadTable);
                List params = new ArrayList();
                memory.pager(query, params, loadSize, pageNo[0]);
                ArrayNode res = memory.query(query, jsonArrayHandler, params);
                Iterator it = res.iterator();
                while (it.hasNext()) {
                    vantiqClient.sendNotification(it.next());
                }
                if (res.size() > 0) {
                    pageNo[0]++;
                }
            }
        };
        // Create new Timer, and schedule the task according to the pollTime
        Timer pollTimer = new Timer("executeLoading");
        pollTimer.schedule(task, 0, loadInterval);
        return pollTimer;
    }

    /**
     * Called by executeQuery() once the query has been executed, and sends the retrieved data back to VANTIQ.
     * @param queryArray     A HashMap Array containing the retrieved data from processQuery().
     * @param message        The Query message
     */
    public void sendDataFromQuery(HashMap[] queryArray, ExtensionServiceMessage message) {
        Map<String, ?> request = (Map<String, ?>) message.getObject();
        String replyAddress = ExtensionServiceMessage.extractReplyAddress(message);

        int bundleFactor = DEFAULT_BUNDLE_SIZE;
        if (request.get("bundleFactor") instanceof Integer && (Integer) request.get("bundleFactor") > -1) {
            bundleFactor = (Integer) request.get("bundleFactor");
        }

        // Send the results of the query
        if (queryArray.length == 0) {
            // If data is empty send empty map with 204 code
            vantiqClient.sendQueryResponse(204, replyAddress, new LinkedHashMap<>());
        } else if (bundleFactor == 0) {
            // If the bundleFactor was specified to be 0, then we sent the entire array
            vantiqClient.sendQueryResponse(200, replyAddress, queryArray);
        } else {
            // Otherwise, send messages containing 'bundleFactor' number of rows
            int len = queryArray.length;
            for (int i = 0; i < len; i += bundleFactor) {
                HashMap[] rowBundle = Arrays.copyOfRange(queryArray, i, Math.min(queryArray.length, i+bundleFactor));

                // If we reached the last row, send with 200 code
                if  (i + bundleFactor >= len) {
                    vantiqClient.sendQueryResponse(200, replyAddress, rowBundle);
                } else {
                    // Otherwise, send row with 100 code signifying more data to come
                    vantiqClient.sendQueryResponse(100, replyAddress, rowBundle);
                }
            }
        }
    }

    @Override
    public void close() {
        this.vantiqClient.close();

        if (scheduledTimer != null) {
            scheduledTimer.cancel();
            scheduledTimer = null;
        }
        synchronized (SYNCH_LOCK) {
            if (jdbc != null) {
                jdbc.close();
                jdbc = null;
            }
        }
    }

    public ExtensionWebSocketClient getVantiqClient() {
        return vantiqClient;
    }

    public ConnectorConfig getConnectionInfo() {
        return connectionInfo;
    }

    public Timer getScheduledTimer() {
        return scheduledTimer;
    }

    public void setScheduledTimer(Timer scheduledTimer) {
        this.scheduledTimer = scheduledTimer;
    }

    public JDBC getJdbc() {
        return jdbc;
    }

    public void setJdbc(JDBC jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Waits for the connection to succeed or fail, logs and exits if the connection does not succeed within
     * {@code timeout} seconds.
     *
     * @param client    The client to watch for success or failure.
     * @param timeout   The maximum number of seconds to wait before assuming failure and stopping
     * @return          true if the connection succeeded, false if it failed to connect within {@code timeout} seconds.
     */
    public boolean checkConnectionFails(ExtensionWebSocketClient client, int timeout) {
        boolean sourcesSucceeded = false;
        try {
            sourcesSucceeded = client.getSourceConnectionFuture().get(timeout, TimeUnit.SECONDS);
        }
        catch (TimeoutException e) {
            LOG.error("Timeout: full connection did not succeed within {} seconds: {}", timeout, e);
        }
        catch (Exception e) {
            LOG.error("Exception occurred while waiting for webSocket connection", e);
        }
        if (!sourcesSucceeded) {
            LOG.error("Failed to connect to all sources.");
            if (!client.isOpen()) {
                LOG.error("Failed to connect to server url '" + connectionInfo.getVantiqUrl() + "'.");
            } else if (!client.isAuthed()) {
                LOG.error("Failed to authenticate within " + timeout + " seconds using the given authentication data.");
            } else {
                LOG.error("Failed to connect within 10 seconds");
            }
            return false;
        }
        return true;

    }


}
