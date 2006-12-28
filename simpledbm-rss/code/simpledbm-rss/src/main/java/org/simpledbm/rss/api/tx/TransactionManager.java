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
 *    Project: www.simpledbm.org
 *    Author : Dibyendu Majumdar
 *    Email  : dibyendu@mazumdar.demon.co.uk
 */
package org.simpledbm.rss.api.tx;

import org.simpledbm.rss.api.wal.Lsn;

/**
 * Defines the interface of the Transaction Manager.
 * 
 * @author Dibyendu Majumdar
 * @since 23-Aug-2005
 */
public interface TransactionManager {

	/** 
	 * Begins a new transaction.
	 */		
	Transaction begin(IsolationMode isolationMode);
	
	/**
	 * Logs an operation that is not part of any specific transaction, but needs to be
	 * redone at system restart. Note that since these updates cannot be tracked via
	 * the status of a disk page, they will always be redone. Also, any actions
	 * prior to the last checkpoint are discarded, therefore, checkpoints should include 
	 * information/data to compensate for this.
	 * <p>
	 * An example of this type of operation is the opening of containers. If a checkpoint
	 * records all open containers, then any containers opened following the checkpoint
	 * can be logged separately. At system restart, containers recorded in the checkpoint 
	 * will be re-opened, and then any containers encountered in the log will be reopened. 
	 */
	Lsn logNonTransactionRelatedOperation(Loggable operation);
	
	/**
	 * Sets the interval at which checkpoints will be taken.
	 * @param millisecs Time interval in milliseconds.
	 */
	void setCheckpointInterval(int millisecs);
	
	/**
	 * Sets the default lock timeopu value.
	 * @param seconds Timeout in seconds.
	 */
	void setLockWaitTimeout(int seconds);
	
	/**
	 * Orchestrates the Transaction Manager restart processing. Also starts any
	 * background threads.
	 */
	void start();
	
	/**
	 * Shutdown the Transaction Manager.
	 */
	void shutdown();
}
