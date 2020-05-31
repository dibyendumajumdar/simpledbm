/**
 * DO NOT REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Contributor(s):
 *
 * The Original Software is SimpleDBM (www.simpledbm.org).
 * The Initial Developer of the Original Software is Dibyendu Majumdar.
 *
 * Portions Copyright 2005-2014 Dibyendu Majumdar. All Rights Reserved.
 *
 * The contents of this file are subject to the terms of the
 * Apache License Version 2 (the "APL"). You may not use this
 * file except in compliance with the License. A copy of the
 * APL may be obtained from:
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the APL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the APL, the GPL or the LGPL.
 *
 * Copies of GPL and LGPL may be obtained from:
 * http://www.gnu.org/licenses/license-list.html
 */
package org.simpledbm.network.server;

import static org.simpledbm.network.server.Messages.LOGGER_NAME;
import static org.simpledbm.network.server.Messages.encodingError;
import static org.simpledbm.network.server.Messages.noActiveTransaction;
import static org.simpledbm.network.server.Messages.noSuchSession;
import static org.simpledbm.network.server.Messages.noSuchTable;
import static org.simpledbm.network.server.Messages.noSuchTableScanMessage;
import static org.simpledbm.network.server.Messages.transactionActive;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.simpledbm.common.api.exception.SimpleDBMException;
import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.api.thread.Scheduler.Priority;
import org.simpledbm.common.util.Dumpable;
import org.simpledbm.common.util.TypeSize;
import org.simpledbm.common.util.logging.Logger;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.database.api.Database;
import org.simpledbm.database.api.DatabaseFactory;
import org.simpledbm.database.api.Table;
import org.simpledbm.database.api.TableScan;
import org.simpledbm.network.common.api.AddRowMessage;
import org.simpledbm.network.common.api.CloseScanMessage;
import org.simpledbm.network.common.api.DeleteRowMessage;
import org.simpledbm.network.common.api.EndTransactionMessage;
import org.simpledbm.network.common.api.FetchNextRowMessage;
import org.simpledbm.network.common.api.FetchNextRowReply;
import org.simpledbm.network.common.api.GetTableMessage;
import org.simpledbm.network.common.api.OpenScanMessage;
import org.simpledbm.network.common.api.QueryDictionaryMessage;
import org.simpledbm.network.common.api.RequestCode;
import org.simpledbm.network.common.api.StartTransactionMessage;
import org.simpledbm.network.common.api.UpdateRowMessage;
import org.simpledbm.network.nio.api.NetworkException;
import org.simpledbm.network.nio.api.Request;
import org.simpledbm.network.nio.api.RequestHandler;
import org.simpledbm.network.nio.api.Response;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.typesystem.api.TableDefinition;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.api.TypeFactory;

public class SimpleDBMRequestHandler implements RequestHandler {

    Database database;
    Platform platform;
    PlatformObjects po;

    Logger log;

    /**
     * Session timeout in seconds, default 300 seconds.
     */
    int timeout;

    /**
     * Intervals at which sessions are checked for timeout.
     */
    int sessionMonitorInterval;

    /**
     * A map of all active sessions
     */
    HashMap<Integer, ClientSession> sessions = new HashMap<Integer, ClientSession>();

    Object sync = new Object();

    /**
     * A sequence number generator used to allocate new session ids.
     */
    AtomicInteger sessionIdGenerator = new AtomicInteger(0);

    ScheduledFuture<?> sessionMonitorFuture;

    /**
     * Checks that the session exists and has not timed out. If the session is
     * good, its last updated time is refreshed.
     */
    private ClientSession validateSession(Request request, Response response) {
        ClientSession session = null;
        synchronized (sync) {
            session = sessions.get(request.getSessionId());
        }
        if (session == null) {
            throw new NetworkException(new MessageInstance(noSuchSession,
                    request.getSessionId()));
        }
        session.checkSessionIsValid();
        session.setLastUpdated();
        return session;
    }

    public void handleRequest(Request request, Response response) {
        if (request.getRequestCode() == RequestCode.OPEN_SESSION) {
            handleOpenSessionRequest(request, response);
        } else if (request.getRequestCode() == RequestCode.CLOSE_SESSION) {
            handleCloseSessionRequest(request, response);
        } else if (request.getRequestCode() == RequestCode.QUERY_DICTIONARY) {
            handleQueryDictionaryRequest(request, response);
        } else if (request.getRequestCode() == RequestCode.CREATE_TABLE) {
            handleCreateTable(request, response);
        } else if (request.getRequestCode() == RequestCode.START_TRANSACTION) {
            handleStartTransaction(request, response);
        } else if (request.getRequestCode() == RequestCode.END_TRANSACTION) {
            handleEndTransaction(request, response);
        } else if (request.getRequestCode() == RequestCode.GET_TABLE) {
            handleGetTable(request, response);
        } else if (request.getRequestCode() == RequestCode.OPEN_TABLESCAN) {
            handleOpenTableScan(request, response);
        } else if (request.getRequestCode() == RequestCode.CLOSE_TABLESCAN) {
            handleCloseTableScan(request, response);
        } else if (request.getRequestCode() == RequestCode.ADD_ROW) {
            handleAddRow(request, response);
        } else if (request.getRequestCode() == RequestCode.FETCH_NEXT_ROW) {
            handleFetchNextRow(request, response);
        } else if (request.getRequestCode() == RequestCode.UPDATE_CURRENT_ROW) {
            handleUpdateCurrentRow(request, response);
        } else if (request.getRequestCode() == RequestCode.DELETE_CURRENT_ROW) {
            handleDeleteCurrentRow(request, response);
        } else {
            handleUnknownRequest(request, response);
        }
    }

    public void onInitialize(Platform platform, Properties properties) {
        this.platform = platform;
        this.po = platform.getPlatformObjects(LOGGER_NAME);
        this.log = po.getLogger();
        // default timeout is 300 seconds
        this.timeout = Integer.parseInt(properties.getProperty(
                "network.server.sessionTimeout", "300000"));
        // default interval is 120 seconds
        this.sessionMonitorInterval = Integer.parseInt(properties.getProperty(
                "network.server.sessionMonitorInterval", "120"));

        database = DatabaseFactory.getDatabase(platform, properties);
        sessionMonitorFuture = platform.getScheduler().scheduleWithFixedDelay(
                Priority.NORMAL, new SessionMonitor(this),
                sessionMonitorInterval, sessionMonitorInterval,
                TimeUnit.SECONDS);
    }

    public void onShutdown() {
        // stop scheduling 
        sessionMonitorFuture.cancel(false);
        // abort any sessions that are still open
        abortSessions();
        database.shutdown();
    }

    public void onStart() {
        database.start();
    }

    private void setError(Response response, int statusCode, String message) {
        response.setStatusCode(statusCode);
        byte[] bytes;
        bytes = message.getBytes(StandardCharsets.UTF_8);
        ByteBuffer data = ByteBuffer.wrap(bytes);
        data.limit(bytes.length);
        response.setData(data);
    }

    private void formatException(StringBuilder sb, Throwable e) {
        sb.append(e.getClass().getName());
        sb.append(": ");
        sb.append(e.getMessage());
        sb.append(Dumpable.newline);
        for (StackTraceElement se : e.getStackTrace()) {
            sb.append(Dumpable.TAB);
            sb.append("at ");
            sb.append(se.toString());
            sb.append(Dumpable.newline);
        }
    }

    void setError(Response response, int statusCode, String message, Throwable e) {
        if (e instanceof SimpleDBMException) {
            throw (SimpleDBMException) e;
        }
        response.setStatusCode(statusCode);
        StringBuilder sb = new StringBuilder();
        sb.append(message);
        sb.append(Dumpable.newline);
        do {
            formatException(sb, e);
            e = e.getCause();
            if (e != null) {
                sb.append("Caused by: ");
            }
        } while (e != null);
        byte[] bytes;
        bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        ByteBuffer data = ByteBuffer.wrap(bytes);
        data.limit(bytes.length);
        response.setData(data);
    }

    void handleOpenSessionRequest(Request request, Response response) {
        int sessionId = sessionIdGenerator.incrementAndGet();
        ClientSession session = new ClientSession(this, sessionId, database);
        synchronized (sync) {
            sessions.put(sessionId, session);
        }
        response.setSessionId(sessionId);
        if (log.isDebugEnabled()) {
            log.debug(getClass(), "handleOpenSessionRequest",
                    "SIMPLEDBM-DEBUG: Opened session " + session);
        }
    }

    void handleCloseSessionRequest(Request request, Response response) {
        int sessionId = request.getSessionId();
        if (log.isDebugEnabled()) {
            log.debug(getClass(), "handleCloseSessionRequest",
                    "SIMPLEDBM-DEBUG: Closing session " + sessionId);
        }
        ClientSession session = null;
        synchronized (sync) {
            session = sessions.get(sessionId);
            if (session == null) {
                throw new NetworkException(new MessageInstance(noSuchSession,
                        sessionId));
            } else {
                sessions.remove(sessionId);
            }
        }
        session.abortTransaction();
        response.setSessionId(0);
    }

    void handleQueryDictionaryRequest(Request request, Response response) {
        QueryDictionaryMessage message = new QueryDictionaryMessage(request
                .getData());
        TypeDescriptor[] td = database.getDictionaryCache().getTypeDescriptor(
                message.getContainerId());
        ByteBuffer bb = ByteBuffer.allocate(database.getTypeFactory()
                .getStoredLength(td));
        database.getTypeFactory().store(td, bb);
        bb.flip();
        response.setData(bb);
    }

    void handleUnknownRequest(Request request, Response response) {
        int sessionId = request.getSessionId();
        setError(response, -1, "Received invalid request "
                + request.getRequestCode() + " from " + sessionId);
        response.setSessionId(0);
    }

    void handleCreateTestTables(Request request, Response response) {
        TypeFactory ff = database.getTypeFactory();
        TypeDescriptor employee_rowtype[] = { ff.getIntegerType(), /*
                                                                    * primary
                                                                    * key
                                                                    */
        ff.getVarcharType(20), /* name */
        ff.getVarcharType(20), /* surname */
        ff.getVarcharType(20), /* city */
        ff.getVarcharType(45), /* email address */
        ff.getDateTimeType(), /* date of birth */
        ff.getNumberType(2) /* salary */
        };
        TableDefinition tableDefinition = database.newTableDefinition(
                "employee", 1, employee_rowtype);
        tableDefinition.addIndex(2, "employee1.idx", new int[] { 0 }, true,
                true);
        tableDefinition.addIndex(3, "employee2.idx", new int[] { 2, 1 }, false,
                false);
        tableDefinition.addIndex(4, "employee3.idx", new int[] { 5 }, false,
                false);
        tableDefinition.addIndex(5, "employee4.idx", new int[] { 6 }, false,
                false);

        database.createTable(tableDefinition);
    }

    void handleCreateTable(Request request, Response response) {
        ClientSession session = validateSession(request, response);
        if (session == null) {
            return;
        }
        TableDefinition tableDefinition = database.getTypeSystemFactory()
                .getTableDefinition(database.getPlatformObjects(),
                        database.getTypeFactory(), database.getRowFactory(),
                        request.getData());
        database.createTable(tableDefinition);
    }

    void handleStartTransaction(Request request, Response response) {
        ClientSession session = validateSession(request, response);
        Transaction transaction = session.getTransaction();
        if (transaction != null) {
            throw new NetworkException(new MessageInstance(transactionActive,
                    transaction));
        }
        StartTransactionMessage message = new StartTransactionMessage(request
                .getData());
        transaction = database.startTransaction(message.getIsolationMode());
        session.setTransaction(transaction);
        if (log.isDebugEnabled()) {
            log.debug(getClass(), "handleStartTransaction",
                    "SIMPLEDBM-DEBUG: Starting transaction for session "
                            + session);
        }
    }

    void handleEndTransaction(Request request, Response response) {
        ClientSession session = validateSession(request, response);
        Transaction transaction = session.getTransaction();
        if (transaction == null) {
            throw new NetworkException(new MessageInstance(noActiveTransaction));
        }
        EndTransactionMessage message = new EndTransactionMessage(request
                .getData());
        if (message.isCommit()) {
            if (log.isDebugEnabled()) {
                log.debug(getClass(), "handleEndTransaction",
                        "SIMPLEDBM-DEBUG: Committing transaction for session "
                                + session);
            }
            session.commitTransaction();
        } else {
            if (log.isDebugEnabled()) {
                log.debug(getClass(), "handleEndTransaction",
                        "SIMPLEDBM-DEBUG: Aborting transaction for session "
                                + session);
            }
            session.abortTransaction();
        }
    }

    void handleGetTable(Request request, Response response) {
        ClientSession session = validateSession(request, response);
        Transaction transaction = session.getTransaction();
        if (transaction == null) {
            throw new NetworkException(new MessageInstance(noActiveTransaction));
        }
        GetTableMessage message = new GetTableMessage(request.getData());
        Table table = session.getTable(message.getContainerId());
        TableDefinition tableDefinition = table.getDefinition();
        ByteBuffer bb = ByteBuffer.allocate(tableDefinition.getStoredLength());
        tableDefinition.store(bb);
        bb.flip();
        response.setData(bb);
        if (log.isDebugEnabled()) {
            log.debug(getClass(), "handleGetTable",
                    "SIMPLEDBM-DEBUG: Opening table "
                            + message.getContainerId() + " for session "
                            + session);
        }
    }

    void handleOpenTableScan(Request request, Response response) {
        ClientSession session = validateSession(request, response);
        Transaction transaction = session.getTransaction();
        if (transaction == null) {
            throw new NetworkException(new MessageInstance(noActiveTransaction));
        }
        OpenScanMessage message = new OpenScanMessage(database.getRowFactory(),
                request.getData());
        Table table = session.getTable(message.getContainerId());
        if (table == null) {
            throw new NetworkException(new MessageInstance(noSuchTable, message
                    .getContainerId()));
        }
        if (log.isDebugEnabled()) {
            log.debug(getClass(), "handleOpenTableScan",
                    "SIMPLEDBM-DEBUG: Open scan for table "
                            + message.getContainerId() + " for session "
                            + session);
        }
        TableScan tableScan = table.openScan(transaction, message.getIndexNo(),
                message.getStartRow(), message.isForUpdate());
        int scanId = session.registerTableScan(tableScan);
        if (log.isDebugEnabled()) {
            log.debug(getClass(), "handleOpenTableScan",
                    "SIMPLEDBM-DEBUG: Scan id " + scanId + " opened for table "
                            + message.getContainerId() + " for session "
                            + session);
        }
        ByteBuffer bb = ByteBuffer.allocate(TypeSize.INTEGER);
        bb.putInt(scanId);
        bb.flip();
        response.setData(bb);
    }

    void handleCloseTableScan(Request request, Response response) {
        ClientSession session = validateSession(request, response);
        Transaction transaction = session.getTransaction();
        if (transaction == null) {
            throw new NetworkException(new MessageInstance(noActiveTransaction));
        }
        CloseScanMessage message = new CloseScanMessage(request.getData());
        session.closeTableScan(message.getScanId());
        if (log.isDebugEnabled()) {
            log.debug(getClass(), "handleCloseTableScan",
                    "SIMPLEDBM-DEBUG: Closed scan " + message.getScanId()
                            + " for session " + session);
        }
    }

    void handleAddRow(Request request, Response response) {
        ClientSession session = validateSession(request, response);
        Transaction transaction = session.getTransaction();
        if (transaction == null) {
            throw new NetworkException(new MessageInstance(noActiveTransaction));
        }
        AddRowMessage message = new AddRowMessage(database.getRowFactory(),
                request.getData());
        Table table = session.getTable(message.getContainerId());
        if (table == null) {
            throw new NetworkException(new MessageInstance(noSuchTable, message
                    .getContainerId()));
        }
        if (log.isDebugEnabled()) {
            log.debug(getClass(), "handleAddRow",
                    "SIMPLEDBM-DEBUG: Added row " + message.getRow()
                            + " to table " + message.getContainerId()
                            + " for session " + session);
        }
        table.addRow(transaction, message.getRow());
    }

    void handleFetchNextRow(Request request, Response response) {
        ClientSession session = validateSession(request, response);
        Transaction transaction = session.getTransaction();
        if (transaction == null) {
            throw new NetworkException(new MessageInstance(noActiveTransaction));
        }
        FetchNextRowMessage message = new FetchNextRowMessage(database
                .getRowFactory(), request.getData());
        TableScan tableScan = session.getTableScan(message.getScanId());
        if (tableScan == null) {
            throw new NetworkException(new MessageInstance(
                    noSuchTableScanMessage, message.getScanId()));
        }
        FetchNextRowReply reply = null;
        boolean hasNext = tableScan.fetchNext();
        if (hasNext) {
            if (log.isDebugEnabled()) {
                log.debug(getClass(), "handleFetchNextRow",
                        "SIMPLEDBM-DEBUG: Fetched row "
                                + tableScan.getCurrentRow()
                                + " from table "
                                + tableScan.getTable().getDefinition()
                                        .getContainerId() + " for session "
                                + session);
            }
            reply = new FetchNextRowReply(tableScan.getTable().getDefinition()
                    .getContainerId(), false, tableScan.getCurrentRow());
        } else {
            if (log.isDebugEnabled()) {
                log.debug(getClass(), "handleFetchNextRow",
                        "SIMPLEDBM-DEBUG: Fetch request reached EOF from table "
                                + tableScan.getTable().getDefinition()
                                        .getContainerId() + " for session "
                                + session);
            }
            reply = new FetchNextRowReply(tableScan.getTable().getDefinition()
                    .getContainerId(), true, null);
        }
        if (log.isDebugEnabled()) {
            log.debug(getClass(), "handleFetchNextRow",
                    "SIMPLEDBM-DEBUG: Fetch result " + reply + " for session "
                            + session);
        }
        ByteBuffer bb = ByteBuffer.allocate(reply.getStoredLength());
        reply.store(bb);
        bb.flip();
        response.setData(bb);
    }

    void handleUpdateCurrentRow(Request request, Response response) {
        ClientSession session = validateSession(request, response);
        Transaction transaction = session.getTransaction();
        if (transaction == null) {
            throw new NetworkException(new MessageInstance(noActiveTransaction));
        }
        UpdateRowMessage message = new UpdateRowMessage(database
                .getRowFactory(), request.getData());
        TableScan tableScan = session.getTableScan(message.getScanId());
        if (tableScan == null) {
            throw new NetworkException(new MessageInstance(
                    noSuchTableScanMessage, message.getScanId()));
        }
        if (log.isDebugEnabled()) {
            log.debug(getClass(), "handleUpdateCurrentRow",
                    "SIMPLEDBM-DEBUG: Updating row "
                            + message.getRow()
                            + " in table "
                            + tableScan.getTable().getDefinition()
                                    .getContainerId() + " for session "
                            + session);
        }
        tableScan.updateCurrentRow(message.getRow());
    }

    /**
     * Process a delete row request
     */
    void handleDeleteCurrentRow(Request request, Response response) {
        ClientSession session = validateSession(request, response);
        Transaction transaction = session.getTransaction();
        if (transaction == null) {
            throw new NetworkException(new MessageInstance(noActiveTransaction));
        }
        DeleteRowMessage message = new DeleteRowMessage(request.getData());
        TableScan tableScan = session.getTableScan(message.getScanId());
        if (tableScan == null) {
            throw new NetworkException(new MessageInstance(
                    noSuchTableScanMessage, message.getScanId()));
        }
        if (log.isDebugEnabled()) {
            log.debug(getClass(), "handleDeleteCurrentRow",
                    "SIMPLEDBM-DEBUG: Deleting row "
                            + tableScan.getCurrentRow()
                            + " from table "
                            + tableScan.getTable().getDefinition()
                                    .getContainerId() + " for session "
                            + session);
        }
        tableScan.deleteRow();
    }

    /*
     * On shutdown we must abort transactions that weren't committed by
     * respective clients We should also periodically check on the session
     * activity and timeout sessions that are inactive for a while.
     */

    void abortSessions() {
        HashMap<Integer, ClientSession> oldsessions = null;
        synchronized (sync) {
            oldsessions = sessions;
            sessions = new HashMap<Integer, ClientSession>();
        }
        for (ClientSession session : oldsessions.values()) {
            try {
                if (log.isDebugEnabled()) {
                    log.debug(getClass(), "abortSessions",
                            "SIMPLEDBM-DEBUG: Aborting transaction for session "
                                    + session);
                }
                session.abortTransaction();
            } catch (Throwable e) {
                log.error(getClass(), "abortSessions",
                        "Unexpected error occurred when aborting session: ", e);
            }
        }
    }

    /**
     * Time out sessions that have expired - have been idle too long. Any
     * pending transactions started by timeout sessions should be aborted.
     * 
     */
    void timeoutSessions() {
        synchronized (sync) {
            Iterator<ClientSession> iter = sessions.values().iterator();
            while (iter.hasNext()) {
                ClientSession session = iter.next();
                try {
                    if (session.checkTimeout(timeout)) {
                        if (log.isDebugEnabled()) {
                            log.debug(getClass(), "timeoutSessions",
                                    "SIMPLEDBM-DEBUG: Timing out session "
                                            + session);
                        }
                        session.abortTransaction();
                        iter.remove();
                    }
                } catch (Throwable e) {
                    log
                            .error(
                                    getClass(),
                                    "timeoutSessions",
                                    "Unexpected error occurred when timing out session: ",
                                    e);
                }
            }
        }
    }

    /**
     * The SessionMonitor times out sessions and removes them from the session
     * cache. This task should be executed periodically.
     * 
     * @author dibyendumajumdar
     */
    static final class SessionMonitor implements Runnable {

        final SimpleDBMRequestHandler myHandler;

        SessionMonitor(SimpleDBMRequestHandler handler) {
            this.myHandler = handler;
        }

        public void run() {
            myHandler.timeoutSessions();
        }
    }
}
