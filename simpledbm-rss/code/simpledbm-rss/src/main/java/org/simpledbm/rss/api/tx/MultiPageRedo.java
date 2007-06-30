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

import org.simpledbm.rss.api.pm.PageId;

/**
 * Extends the standard Redoable interface to allow the Log Operation to cover
 * multiple pages. At present only Redoable and Compensation records can use this
 * interface.  
 */
public interface MultiPageRedo extends Redoable {

    /**
     * Returns the IDs of the pages that are affected by the log record.
     * Must include the default page return by {@link Loggable#getPageId()}.
     * During redo, the log record will be applied to each of the pages in this
     * list. Hence redo implementation must check the page the log record is
     * being applied and accordingly apply the changes.
     */
    public PageId[] getPageIds();
}
