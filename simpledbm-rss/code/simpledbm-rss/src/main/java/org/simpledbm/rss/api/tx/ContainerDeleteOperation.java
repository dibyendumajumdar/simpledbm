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
 * Defines the interface for the Container Delete operation.
 * ARIES requires container delete operations to be marked so that 
 * during the restart analysis phase, all pages related to the deleted container
 * can be removed from dirty pages table. Also, the container itself is
 * closed and removed from StorageManager to disallow further updates to it.
 * <p>
 * Constraints: Must not be part of the Redoable hierarchy.
 * 
 * @author Dibyendu Majumdar
 * @since 26-Aug-2005
 */
public interface ContainerDeleteOperation extends Loggable {
    /**
     * Returns the ID of the container that is to be deleted. 
     */
    int getContainerId();
}
