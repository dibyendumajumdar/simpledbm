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
package org.simpledbm.rss.api.fsm;

/**
 * Provides an interface for scanning for non-empty pages within a container.
 * Note that the scan may return pages that are not data pages; the caller must
 * check that the page is of the appropriate type. Pages are returned in order
 * from start of the container. 
 * 
 * @author Dibyendu Majumdar
 * @since 09-Feb-2006
 */
public interface FreeSpaceScan {

    /**
     * Determines the next page within the container that satisfies the 
     * search criteria. Note that the returned page may not be a data page; 
     * the caller must check that the page is of the appropriate type.
     */
    boolean fetchNext();

    /**
     * If fetchNext() was successful, return the page number.
     */
    int getCurrentPage();

    /**
     * Returns the EOF status of the scan.
     */
    boolean isEof();

    /**
     * Closes the scan and releases any resources occupied by the scan.
     */
    void close();
}
