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
 * 
 * <p>Defines the interface to low level IO sub-system. The objective is to
 * present a generic interface that can potentially be implemented in a
 * number of different ways. The default implementation is in {@link org.simpledbm.rss.impl.st}.</p>
 * <h2>Overview</h2>
 * <p>Database Managers typically use files to store various types of data,
 * such as, log files, data files, etc. However, from the perspective of a DBMS,
 * the concept of a file is a logical one; all the DBMS cares about is a named storage
 * container that supports random positioned IO. As long as this requirement is met, it is
 * not important whether a container maps to a file or to some other device.</p>
 * <p>The objective of this package is to provide a level of abstraction to the rest of the DBMS so that
 * the mapping of a container to a file becomes an implementation artifact. If desired,
 * containers may be mapped to raw devices, or to segments within a file.</p>
 * <p>Container names are usually not good identifiers for the rest of the system.
 * Integer identifiers are better, especially when other objects need to refer to
 * specific containers. Integers take less amount of storage, and also remove the
 * dependency between the container's name and the rest of the system. To support this
 * requirement, the {@link org.simpledbm.rss.api.st.StorageManager} interface is provided, which maintains a 
 * mapping of StorageContainers to integer identifiers. Note that the Storage
 * sub-system does not decide how to map the containers to ids; it enables the 
 * registration of these mappings and allows StorageContainer objects to be retrieved
 * using their numeric identifiers.
 */
package org.simpledbm.rss.api.st;

