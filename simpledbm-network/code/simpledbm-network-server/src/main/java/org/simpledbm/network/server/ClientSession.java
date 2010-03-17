/***
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program; if not, write to the Free Software
 *    Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *
 *    Linking this library statically or dynamically with other modules 
 *    is making a combined work based on this library. Thus, the terms and
 *    conditions of the GNU General Public License cover the whole
 *    combination.
 *
 *    As a special exception, the copyright holders of this library give 
 *    you permission to link this library with independent modules to 
 *    produce an executable, regardless of the license terms of these 
 *    independent modules, and to copy and distribute the resulting 
 *    executable under terms of your choice, provided that you also meet, 
 *    for each linked independent module, the terms and conditions of the 
 *    license of that module.  An independent module is a module which 
 *    is not derived from or based on this library.  If you modify this 
 *    library, you may extend this exception to your version of the 
 *    library, but you are not obligated to do so.  If you do not wish 
 *    to do so, delete this exception statement from your version.
 *
 *    Project: www.simpledbm.org
 *    Author : Dibyendu Majumdar
 *    Email  : d dot majumdar at gmail dot com ignore
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
     * The requestHandler that is managing this sessoion.
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

    Transaction getTransaction() {
        return transaction;
    }

    synchronized void setLastUpdated() {
        lastUpdated = System.currentTimeMillis();
    }

    synchronized void setTransaction(Transaction transaction) {
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

    int getSessionId() {
        return sessionId;
    }

    synchronized Table getTable(int containerId) {
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

    synchronized int registerTableScan(TableScan scan) {
        if (transaction == null) {
            throw new NetworkException(new MessageInstance(noActiveTransaction));
        }
        int s = scanId++;
        tableScans.put(s, scan);
        return s;
    }

    synchronized TableScan getTableScan(int scanId) {
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

    synchronized void closeTableScan(int scanId) {
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

    synchronized void closeScans() {
        for (TableScan scan : tableScans.values()) {
            try {
                scan.close();
            } catch (Throwable e) {
                log.error(getClass().getName(), "closeScans",
                        new MessageInstance(unexpectedError).toString(), e);
            }
        }
        tableScans.clear();
    }

    synchronized void abortTransaction() {
        if (transaction != null) {
            closeScans();
            tables.clear();
            transaction.abort();
            transaction = null;
        }
    }

    synchronized void commitTransaction() {
        if (transaction != null) {
            closeScans();
            tables.clear();
            transaction.commit();
            transaction = null;
        }
    }

    synchronized boolean checkTimeout(int timeout) {
        if (timedOut) {
            return true;
        }
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastUpdated > timeout) {
            timedOut = true;
        }
        return timedOut;
    }

}
