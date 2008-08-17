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
 * <p>Defines the interface for the Page Management module.</p>
 * <h2>Overview of Page Manager module</h2>
 * <p>The database storage system is managed in units of IO called pages.
 * A page is typically a fixed size block within the storage container.
 * The Page Manager module encapsulates the knowledge about how pages
 * map to containers. It knows about page sizes, and also knows how to
 * read/write pages from storage containers. By isolating this knowledge
 * into a separate module, the rest of the system is protected. For example,
 * the Buffer Manager module can work with different paging strategies
 * by switching the Page Manager module.</p>
 * <p>Note that the Page Manager module does not worry about the contents
 * of the page, except for the very basic and common stuff that must be part
 * of every page, such as page Id, page LSN, and page type. It is expected that
 * other modules will extend the basic page type and implement additional
 * features.</p>
 * <h2>Interactions with other modules</h2>
 * <p>The Buffer Manager module uses the Page Manager module to read/write
 * pages from storage containers and also to create new instances of pages.</p>
 * <p>The Page Manager module requires the services of the Object Registry
 * module in order to create instances of pages from type codes.
 * </p>
 * <p>Page Manager module also interacts with the Storage Manager module
 * for access to Storage Containers.</p>
 */
package org.simpledbm.rss.api.pm;

