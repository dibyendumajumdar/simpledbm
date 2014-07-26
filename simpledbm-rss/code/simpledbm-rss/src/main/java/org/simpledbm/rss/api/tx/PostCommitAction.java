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

/**
 * Post commit actions are used to schedule actions that must be deferred until
 * the transaction completes. An example would be the physical dropping of a
 * container.
 * <p>
 * Although PostCommitActions are scheduled as part of a transaction, they are
 * handled somewhat differently from other transaction related records. All
 * PostCommitActions are logged as part of the transaction's Prepare log record.
 * After the transaction's Commit/End record is logged, PostCommitActions are
 * performed. Each PostCommitAction is individually logged after it has been
 * performed. The logging is done to allow the Transaction Manager to determine
 * which actions have been performed and which are still pending. Note that
 * these log records are not added to the list of Transaction's log records.
 * <p>
 * At system restart, when the TM encounters a Prepare record, it makes a list
 * of the associated PostCommitActions. If the transaction is committed, then
 * the TM checks whether these actions have been performed, by comparing the
 * list of pending actions with the logged actions. Those actions that have been
 * performed are discarded, while the remaining actions are performed.
 * <p>
 * A constraint is that the PostCommitActions must not be done when a Checkpoint
 * is active. SimpleDBM's transaction manager ensures this ensuring that
 * checkpoints take an exclusive lock on the transaction manager.
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
