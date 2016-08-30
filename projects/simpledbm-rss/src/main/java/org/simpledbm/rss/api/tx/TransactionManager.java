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
package org.simpledbm.rss.api.tx;

import org.simpledbm.common.api.tx.IsolationMode;
import org.simpledbm.rss.api.wal.Lsn;

/**
 * Defines the interface of the Transaction Manager.
 * 
 * @author Dibyendu Majumdar
 * @since 23-Aug-2005
 */
public interface TransactionManager {

    public final String LOGGER_NAME = "org.simpledbm.transactionmgr";

    /**
     * Begins a new transaction.
     */
    Transaction begin(IsolationMode isolationMode);

    /**
     * Logs an operation that is not part of any specific transaction, but needs
     * to be redone at system restart. Note that since these updates cannot be
     * tracked via the status of a disk page, they will always be redone. Also,
     * any actions prior to the last checkpoint are discarded, therefore,
     * checkpoints should include information/data to compensate for this.
     * <p>
     * An example of this type of operation is the opening of containers. If a
     * checkpoint records all open containers, then any containers opened
     * following the checkpoint can be logged separately. At system restart,
     * containers recorded in the checkpoint will be re-opened, and then any
     * containers encountered in the log will be reopened.
     */
    Lsn logNonTransactionRelatedOperation(Loggable operation);

    /**
     * Sets the interval at which checkpoints will be taken.
     * 
     * @param millisecs Time interval in milliseconds.
     */
    void setCheckpointInterval(int millisecs);

    /**
     * Sets the default lock timeopu value.
     * 
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
