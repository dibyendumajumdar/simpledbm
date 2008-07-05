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
 *    Email  : d dot majumdar at gmail dot com ignore
 */
package org.simpledbm.rss.api.tx;

/**
 * Marker interface for log records that are not related to
 * any specific transaction. Note that the Transaction Manager cannot track
 * the status of such records; that is, it cannot determine whether the
 * log record has been applied or not. As a result, such log records are
 * applied unconditionally.
 * <p>
 * Another important point to note is that such records are discarded
 * after a Checkpoint. This means that only those log records will be applied at
 * system restart that are found after the last Checkpoint. It is assumed that
 * checkpoint records contain the effects of all past log records of this type.
 * For example, checkpoints contain a list of open containers. Any containers
 * opened after the last checkpoint can be logged using a NonTransactionRelatedOperation.
 * These records will be replayed at system restart, until the next Checkpoint when 
 * the updated open containers list will be recorded in the checkpoint record.
 *  
 * @author Dibyendu Majumdar
 * @since 26-Aug-2005
 * @see TransactionManager#logNonTransactionRelatedOperation(Redoable)
 */
public interface NonTransactionRelatedOperation extends Loggable {

}
