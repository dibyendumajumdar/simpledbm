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

import static org.simpledbm.network.server.Messages.unexpectedError;
import static org.simpledbm.network.server.Messages.noSuchTable;
import static org.simpledbm.network.server.Messages.noSuchTableScanMessage;
import static org.simpledbm.network.server.Messages.timedOutMessage;
import static org.simpledbm.network.server.Messages.noActiveTransaction;

import java.util.HashMap;

import org.simpledbm.common.util.logging.Logger;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.database.api.Database;
import org.simpledbm.database.api.Table;
import org.simpledbm.database.api.TableScan;
import org.simpledbm.network.nio.api.NetworkException;
import org.simpledbm.rss.api.tx.Transaction;

/**
 * Represents a session from the client.
 * 
 * @author dibyendu majumdar
 */
public class ClientSession {

    /**
     * The requestHandler that is managing this session.
     */
    final SimpleDBMRequestHandler requestHandler;

    /**
     * Unique session id.
     */
    final int sessionId;

    /**
     * The database we are associated with.
     */
    final Database database;

    /**
     * Cached tables related to the current transaction.
     */
    final HashMap<Integer, Table> tables = new HashMap<Integer, Table>();

    /**
     * Cached table scans related to the current transaction.
     */
    final HashMap<Integer, TableScan> tableScans = new HashMap<Integer, TableScan>();

    /**
     * Each scan is allocated a unique id within the context of the session.
     */
    volatile int scanId = 1;

    /**
     * Current transaction. Session can have only one transaction active.
     */
    volatile Transaction transaction;

    /**
     * Tracks the last time any activity happened on the session.
     */
    volatile long lastUpdated;

    /**
     * If set indicates that the session has timed out.
     */
    volatile boolean timedOut;

    final Logger log;

    /**
     * Check the session for timeout etc.
     */
    void checkSessionIsValid() {
        if (timedOut) {
            throw new NetworkException(new MessageInstance(timedOutMessage));
        }
    }

    final synchronized Transaction getTransaction() {
        return transaction;
    }

    final synchronized void setLastUpdated() {
        lastUpdated = System.currentTimeMillis();
    }

    final synchronized void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    ClientSession(SimpleDBMRequestHandler requestHandler, int sessionId,
            Database database) {
        this.requestHandler = requestHandler;
        this.sessionId = sessionId;
        this.database = database;
        this.lastUpdated = System.currentTimeMillis();
        this.timedOut = false;
        this.log = requestHandler.po.getLogger();
    }

    final int getSessionId() {
        return sessionId;
    }

    final synchronized Table getTable(int containerId) {
        if (transaction == null) {
            throw new NetworkException(new MessageInstance(noActiveTransaction));
        }
        Table table = null;
        table = tables.get(containerId);
        if (table == null) {
            table = database.getTable(transaction, containerId);
            if (table == null) {
                throw new NetworkException(new MessageInstance(noSuchTable,
                        containerId));
            }
            tables.put(containerId, table);
        }
        return table;
    }

    final synchronized int registerTableScan(TableScan scan) {
        if (transaction == null) {
            throw new NetworkException(new MessageInstance(noActiveTransaction));
        }
        int s = scanId++;
        tableScans.put(s, scan);
        return s;
    }

    final synchronized TableScan getTableScan(int scanId) {
        if (transaction == null) {
            throw new NetworkException(new MessageInstance(noActiveTransaction));
        }
        TableScan scan = tableScans.get(scanId);
        if (scan == null) {
            throw new NetworkException(new MessageInstance(
                    noSuchTableScanMessage, scanId));
        }
        return scan;
    }

    final synchronized void closeTableScan(int scanId) {
        if (transaction == null) {
            throw new NetworkException(new MessageInstance(noActiveTransaction));
        }
        TableScan scan = tableScans.remove(scanId);
        if (scan == null) {
            throw new NetworkException(new MessageInstance(
                    noSuchTableScanMessage, scanId));
        }
        scan.close();
    }

    final synchronized void closeScans() {
        for (TableScan scan : tableScans.values()) {
            try {
                scan.close();
            } catch (Throwable e) {
                log.error(getClass(), "closeScans",
                        new MessageInstance(unexpectedError).toString(), e);
            }
        }
        tableScans.clear();
    }

    final synchronized void abortTransaction() {
        if (transaction != null) {
            closeScans();
            tables.clear();
            transaction.abort();
            transaction = null;
        }
    }

    final synchronized void commitTransaction() {
        if (transaction != null) {
            closeScans();
            tables.clear();
            transaction.commit();
            transaction = null;
        }
    }

    final synchronized boolean checkTimeout(int timeout) {
        if (timedOut) {
            return true;
        }
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastUpdated > timeout) {
            timedOut = true;
        }
        return timedOut;
    }

    @Override
    public String toString() {
        return "ClientSession [lastUpdated=" + lastUpdated + ", sessionId="
                + sessionId + ", timedOut=" + timedOut + "]";
    }

}
