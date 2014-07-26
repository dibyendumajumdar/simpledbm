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
package org.simpledbm.typesystem.api;

import java.nio.ByteBuffer;

import org.simpledbm.common.api.key.IndexKeyFactory;

/**
 * A factory for generating rows. Also provides the ability to register row
 * types for containers, keyed by the container id.
 * 
 * @author Dibyendu Majumdar
 * @since 7 May 2007
 */
public interface RowFactory extends IndexKeyFactory {

    /**
     * Creates a new row for a specified container ID. The container ID is used
     * to locate the type information for the row.
     */
    Row newRow(int containerId);

    /**
     * Creates a new row for a specified container ID. The container ID is used
     * to locate the type information for the row.
     */
    Row newRow(int containerId, ByteBuffer bb);

    /**
     * Retrieves the DictionaryCache associated with this Row Factory.
     */
    //	DictionaryCache getDictionaryCache();

    /**
     * Registers the row definition for a particular container ID.
     * 
     * @param containerId container ID for which the row type information is
     *            being registered
     * @param rowTypeDesc An array of types that describe the fields in the row.
     */
    //	void registerRowType(int containerId, TypeDescriptor[] rowTypeDesc);

    /**
     * Removes the row definition for a particular container ID.
     * 
     * @param containerId container ID for which the row type information is
     *            being removed
     */
    //    void unregisterRowType(int containerId);
}
