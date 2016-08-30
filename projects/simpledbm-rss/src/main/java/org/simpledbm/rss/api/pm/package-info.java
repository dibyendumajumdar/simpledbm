/**
 * DO NOT REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Contributor(s):
 *
 * The Original Software is SimpleDBM (www.simpledbm.org).
 * The Initial Developer of the Original Software is Dibyendu Majumdar.
 *
 * Portions Copyright 2005-2014 Dibyendu Majumdar. All Rights Reserved.
 *
 * The contents of this file are subject to the terms of the
 * Apache License Version 2 (the "APL"). You may not use this
 * file except in compliance with the License. A copy of the
 * APL may be obtained from:
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the APL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the APL, the GPL or the LGPL.
 *
 * Copies of GPL and LGPL may be obtained from:
 * http://www.gnu.org/licenses/license-list.html
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

