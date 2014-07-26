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
 * <p>Defines the interface for the Buffer Manager module.</p>
 * <h2>Overview</h2>
 * <p>The Buffer Manager is a critical component of any DBMS. Its primary job
 * is to cache disk pages in memory. Typically, a Buffer Manager has a fixed
 * size Buffer Pool, implemented as an array of in-memory pages.
 * The contents of the Buffer Pool change over time, as pages are read in, and
 * written out. One of the principle tasks of the Buffer Manager is to decide
 * which page should stay in memory, and which should not. The aim is to try to
 * keep the most frequently required pages in memory. The efficiency of the
 * Buffer Manager can be measured by its cache hit-rate, which is the ratio of pages found 
 * in the cache, to pages accessed by the system.
 * </p>
 * <p>In order to decide which pages to maintain in memory, the Buffer Manager
 * typically implements some form of Least Recently Used (LRU) algorithm. In the simplest
 * form, this is simply a linked list of all cached pages, the head of the list representing the
 * least recently used page, and the tail the most recently used. This is based on 
 * the assumption that if a page was accessed recently, then it is likely to be accessed
 * again soon. Since every time a page is accessed, it is moved to the MRU end of the
 * list, therefore over time, the most frequently accessed pages tend to accumulate on the MRU side.
 * Of course, if a client reads a large number of temporary pages, then this scheme can
 * be upset. To avoid this, the Buffer Manager may support hints, so that a client can
 * provide more information to the Buffer Manager, which can then use this information
 * to improve the page replacement algorithm. An example of such a hint would be to
 * flag temporary pages. The Buffer Manager can then use this knowledge to decide that
 * instead of the page going to MRU end, it goes to the LRU end.</p>
 * <h2>Interactions with other modules</h2>
 * <p>The Buffer Manager interacts with the {@link org.simpledbm.rss.api.wal Log Manager}
 * and the {@link org.simpledbm.rss.api.pm Page Manager} modules. It needs the help of the
 * {@link org.simpledbm.rss.api.pm.PageManager PageFactory} in order to instantiate new pages, read pages from disk, and write out
 * dirty pages to disk. In order to support the Write Ahead Log protocol, the Buffer 
 * Manager must ensure that all logs related to the page in question are flushed prior
 * to the page being persisted to disk.</p>
 * <p>The {@link org.simpledbm.rss.api.tx Transaction Manager} also interacts with the Buffer Manager. During checkpoints,
 * the Transaction Manager asks for a list of dirty pages. It uses information maintained
 * by the Buffer Manager to determine where recovery should start. After a system restart
 * the Transaction Manager informs the Buffer Manager about the recovery status of disk
 * pages.</p>
 */
package org.simpledbm.rss.api.bm;

