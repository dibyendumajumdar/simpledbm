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
 * Marker interface for log records that require logical undo.
 * This information is used by the TransactionManager to decide how to
 * coordinate the undo.
 * <p>
 * A LogicalUndo is characterized by the fact that the undo operation may
 * be applied to a different page than the redo operation. Also, undos may 
 * result in further updates to unspecified number of pages, which may generate
 * new log records. This happens, for example, when a BTree insert or delete
 * is undone.
 * 
 * @author Dibyendu Majumdar
 * @since 31-Aug-2005
 */
public interface LogicalUndo extends Undoable {

}
