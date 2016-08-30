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

