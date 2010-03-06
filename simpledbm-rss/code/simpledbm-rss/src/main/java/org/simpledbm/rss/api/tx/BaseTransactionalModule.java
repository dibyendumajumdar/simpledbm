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
		throw new UnsupportedOperationException();
	}

	public final BufferAccessBlock findAndFixPageForUndo(Undoable undoable) {
		throw new UnsupportedOperationException();
	}

	public Compensation generateCompensation(Undoable undoable) {
		throw new UnsupportedOperationException();
	}

	public abstract void redo(Page page, Redoable loggable);

	public void redo(Loggable loggable) {
		throw new UnsupportedOperationException();
	}
}
