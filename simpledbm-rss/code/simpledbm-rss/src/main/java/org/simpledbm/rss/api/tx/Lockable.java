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
 * The Lockable interface defines the contract between the
 * Transaction Manager and other modules. All locks that are to be
 * handled by the Transaction Manager must implement this interface.
 * For convenience, the abtract class {@link BaseLockable} is provided.
 * It is recommended that transactional modules extend BaseLockable to 
 * implement their own lockable classes.
 *  
 * @author Dibyendu Majumdar
 * @since 15 Dec 2006
 */
public interface Lockable {

    /**
     * Returns the container ID associated with this lock. 
     */
    int getContainerId();
}
