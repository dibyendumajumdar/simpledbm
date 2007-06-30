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

/**
 * Post commit actions are used to schedule actions that must be
 * deferred until the transaction completes. An example would be the physical dropping of a
 * container.
 * <p>
 * Although PostCommitActions are scheduled as part of a transaction, they are handled somewhat
 * differently from other transaction related records. All PostCommitActions are logged as
 * part of the transaction's Prepare log record. After the transaction's Commit/End record is
 * logged, PostCommitActions are performed. Each PostCommitAction is individually logged after it has been
 * performed. The logging is done to allow the Transaction Manager to determine which actions have
 * been performed and which are still pending. Note that these log records are not 
 * added to the list of Transaction's log records.
 * <p>
 * At system restart, when the TM encounters a Prepare record, it makes a list of the associated
 * PostCommitActions. If the transaction is committed, then the TM checks whether these actions have
 * been performed, by comparing the list of pending actions with the logged actions. Those actions that
 * have been performed are discarded, while the remaining actions are performed.
 * <p>
 * A constraint is that the PostCommitActions must not be done when a Checkpoint is active.
 * SimpleDBM's transaction manager ensures this ensuring that checkpoints take an exclusive
 * lock on the transaction manager. 
 *  
 * @author Dibyendu Majumdar
 * @since 27-Aug-2005
 * @see org.simpledbm.rss.api.tx.Transaction#schedulePostCommitAction(PostCommitAction)
 */
public interface PostCommitAction extends Loggable,
        NonTransactionRelatedOperation {

    /**
     * Returns the ID assigned to this action. 
     */
    public int getActionId();

    /**
     * Sets the ID for this action.
     */
    public void setActionId(int actionId);

}
