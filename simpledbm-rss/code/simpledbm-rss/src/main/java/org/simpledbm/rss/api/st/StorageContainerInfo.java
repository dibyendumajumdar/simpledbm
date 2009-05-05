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
/*
 * Created on: 18-Nov-2005
 * Author: Dibyendu Majumdar
 */
package org.simpledbm.rss.api.st;

import org.simpledbm.common.util.Dumpable;

/**
 * Represents information about an active storage container.
 *
 * @author Dibyendu Majumdar
 * @since 18-Nov-2005
 */
public interface StorageContainerInfo extends Dumpable {

    /**
     * Returns the numeric ID assigned to the storage container.
     */
    int getContainerId();

    /**
     * Returns the name of the container.
     */
    String getName();
}
