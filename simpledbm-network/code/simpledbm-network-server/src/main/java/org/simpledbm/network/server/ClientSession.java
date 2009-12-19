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

import java.util.HashMap;

import org.simpledbm.database.api.Database;
import org.simpledbm.database.api.Table;
import org.simpledbm.database.api.TableScan;
import org.simpledbm.rss.api.tx.Transaction;

public class ClientSession {
    
    final int sessionId;
    Transaction transaction;
    Database database;
    HashMap<Integer, Table> tables = new HashMap<Integer, Table>();
    HashMap<Integer, TableScan> tableScans = new HashMap<Integer, TableScan>();
    int scanId = 1;
    
    public Transaction getTransaction() {
		return transaction;
	}

	public void setTransaction(Transaction transaction) {
		this.transaction = transaction;
	}

	ClientSession(int sessionId, Database database) {
        this.sessionId = sessionId;
        this.database = database;
    }

	public int getSessionId() {
		return sessionId;
	}
	
	Table getTable(int containerId) {
		Table table = null;
		synchronized(tables) {
			table = tables.get(containerId);
			if (table == null) {
				table = database.getTable(transaction, containerId);
				if (table == null) {
					throw new RuntimeException("No table defined");
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
				throw new RuntimeException("No such scan id");
			}
			return scan;
		}
	}
	
	void closeTableScan(int scanId) {
		synchronized(tableScans) {
			TableScan scan = tableScans.remove(scanId);
			if (scan == null) {
				throw new RuntimeException("No such scan id");
			}
			scan.close();
		}
	}
	
}
