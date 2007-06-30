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

import org.simpledbm.rss.api.bm.BufferAccessBlock;
import org.simpledbm.rss.api.pm.Page;

/**
 * Provides a convenient base class for implementing Transactional Modules.
 * 
 * @see TransactionalModule
 * @author Dibyendu Majumdar
 * @since 01-Sep-2005
 */
public abstract class BaseTransactionalModule implements TransactionalModule {

    public void undo(Transaction trx, Undoable undoable) {
        throw new TransactionException();
    }

    public final BufferAccessBlock findAndFixPageForUndo(Undoable undoable) {
        throw new TransactionException();
    }

    public Compensation generateCompensation(Undoable undoable) {
        throw new TransactionException();
    }

    public abstract void redo(Page page, Redoable loggable);

    public void redo(Loggable loggable) {
        throw new TransactionException();
    }

}
