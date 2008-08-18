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

/**
 * <p>The Free Space Manager module is responsible for managing free space information within a Container.</p>
 * <p>A Raw Container as created by the {@link org.simpledbm.rss.api.st.StorageContainerFactory} interface is more or less
 * a stream of bytes. The Free Space Manager module treats the Raw Container as
 * an ordered collection of fixed size {@link org.simpledbm.rss.api.pm.Page Page}s. It also distinguishes between different types of pages. A
 * container managed by the Free Space Manager module contains at least two
 * types of pages. Firstly, special pages called Free Space Map Pages are
 * used to track space allocation within the container. One or two bits are
 * used to track space for each page within the container. Secondly, data
 * pages are available for client modules to use. The type of the data page
 * may be specified when creating a container.</p>
 */
package org.simpledbm.rss.api.fsm;

