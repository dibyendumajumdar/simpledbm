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
package org.simpledbm.rss.api.locking.util;

import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.tx.Lockable;

/**
 * A LockAdaptor encapsulates knowledge about different types of lockable
 * objects. It is responsible for ensuring that different lockable types have
 * different namespaces, for example, locations and containers.
 * 
 * @author Dibyendu Majumdar
 * @since 14 Dec 2006
 */
public interface LockAdaptor {

    /**
     * Creates a lockable object for the specified container id. All container
     * ids should belong to a distinct namespace.
     */
    Lockable getLockableContainerId(int containerId);

    /**
     * Creates a lockable object for the container id associated with the
     * specified location. Note that this method may exhibit implementation
     * specific behaviour.
     * 
     * @param location Location object to be mapped to a container id
     * @throws IllegalArgumentException Thrown if the location argument cannot
     *             be converted to a container id
     */
    Lockable getLockableContainerId(Location location);

}
