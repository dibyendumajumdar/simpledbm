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

import static org.simpledbm.network.server.Messages.noSuchTableMessage;
import static org.simpledbm.network.server.Messages.noSuchTableScanMessage;
import static org.simpledbm.network.server.Messages.timedOutMessage;

import java.util.HashMap;

import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.database.api.Database;
import org.simpledbm.database.api.Table;
import org.simpledbm.database.api.TableScan;
import org.simpledbm.network.nio.api.NetworkException;
import org.simpledbm.rss.api.tx.Transaction;

public class ClientSession {
    
    final int sessionId;
    final Database database;
    final HashMap<Integer, Table> tables = new HashMap<Integer, Table>();
    final HashMap<Integer, TableScan> tableScans = new HashMap<Integer, TableScan>();
    volatile int scanId = 1;
    volatile Transaction transaction;
    volatile long lastUpdated;
    volatile boolean timedOut;  
    
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
	
	ClientSession(int sessionId, Database database) {
        this.sessionId = sessionId;
        this.database = database;
        this.lastUpdated = System.currentTimeMillis();
        this.timedOut = false;
    }

	int getSessionId() {
		return sessionId;
	}
	
	Table getTable(int containerId) {
		Table table = null;
		synchronized(tables) {
			table = tables.get(containerId);
			if (table == null) {
				table = database.getTable(transaction, containerId);
				if (table == null) {
					throw new NetworkException(new MessageInstance(noSuchTableMessage, containerId));
				}
				tables.put(containerId, table);
			}
		}
		return table;
	}
    
	int registerTableScan(TableScan scan) {
		synchronized(tableScans) {
			int s = scanId++;
			tableScans.put(s, scan);
			return s;
		}
	}
	
	TableScan getTableScan(int scanId) {
		synchronized(tableScans) {
			TableScan scan = tableScans.get(scanId);
			if (scan == null) {
				throw new NetworkException(new MessageInstance(noSuchTableScanMessage, scanId));
			}
			return scan;
		}
	}
	
	void closeTableScan(int scanId) {
		synchronized(tableScans) {
			TableScan scan = tableScans.remove(scanId);
			if (scan == null) {
				throw new NetworkException(new MessageInstance(noSuchTableScanMessage, scanId));
			}
			scan.close();
		}
	}
	
	synchronized void closeScans() {
		for (TableScan scan: tableScans.values()) {
			try {
				scan.close();
			}
			catch (Throwable e) {
				// FIXME
				e.printStackTrace();
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
	
	synchronized void checkTimeout(int timeout) {
		if (timedOut) {
			return;
		}
		long currentTime = System.currentTimeMillis();
		if (currentTime - lastUpdated > timeout) {
			timedOut = true;
		}
	}
	
}
