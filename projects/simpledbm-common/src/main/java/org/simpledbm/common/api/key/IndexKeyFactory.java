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
package org.simpledbm.common.api.key;

import java.nio.ByteBuffer;

/**
 * An IndexKeyFactory is responsible for generating keys. This interface is
 * typically implemented by the clients of the IndexManager module.
 * 
 * @author Dibyendu Majumdar
 * @since Oct-2005
 */
public interface IndexKeyFactory {

    /**
     * Generates a new (empty) key for the specified Container. The Container ID
     * can be used to lookup the index key type for a container.
     * 
     * @param containerId ID of the container for which a key is required
     */
    IndexKey newIndexKey(int containerId);

    /**
     * Generates a key that represents Infinity - it must be greater than all
     * possible keys in the domain for the key. The Container ID can be used to
     * lookup the index key type for a container.
     * 
     * @param containerId ID of the container for which a key is required
     */
    IndexKey maxIndexKey(int containerId);

    /**
     * Generates a key that represents negative Infinity - it must be smaller
     * than all possible keys in the domain for the key. The Container ID can be
     * used to lookup the index key type for a container.
     * <p>
     * The key returned by this method can be used as an argument to index
     * scans. The result will be a scan of the index starting from the first key
     * in the index.
     * 
     * @param containerId ID of the container for which a key is required
     */
    IndexKey minIndexKey(int containerId);

    /**
     * Reconstructs an index key from the byte stream represented by the
     * ByteBuffer. The Container ID can be used to lookup the index key type for
     * a container.
     * <p>
     * The IndexKey implementation must provide a constructor that takes a
     * single {@link ByteBuffer} parameter.
     * 
     * @param containerId The ID of the container for which the key is being
     *            read
     * @param bb ByteBuffer representing the byte stream that contains the key
     *            to be read
     * @return A newly instantiated key.
     */
    IndexKey newIndexKey(int containerId, ByteBuffer bb);

    /**
     * Parse the supplied string and construct a key. Not guaranteed to work,
     * but useful for creating test cases.
     */
    IndexKey parseIndexKey(int containerId, String s);
}
