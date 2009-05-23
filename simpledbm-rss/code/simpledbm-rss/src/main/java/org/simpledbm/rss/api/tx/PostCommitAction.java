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
