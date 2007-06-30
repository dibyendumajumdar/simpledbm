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
 * Interface definition for Compensation log records. Compensation log
 * records are used to log Undo operations. For example, if a transaction changed
 * A to B and then aborted, then the change from B to A is recorded as a
 * Compensation log record. It is redo only, which means that a Compensation log
 * record will never be undone. 
 * <p>In Compensation log records, the undoNextLsn is set to the lsn of the 
 * predecessor of the log record being compensated. 
 * 
 * @author Dibyendu Majumdar
 * @since 23-Aug-2005
 */
public interface Compensation extends Redoable {

    /**
     * Sets pointer to the next record that should be undone. 
     */
    public Lsn getUndoNextLsn();

    /**
     * Gets pointer to the next record that should be undone. 
     */
    public void setUndoNextLsn(Lsn undoNextLsn);

}
