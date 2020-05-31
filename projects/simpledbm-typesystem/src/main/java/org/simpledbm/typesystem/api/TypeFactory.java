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

/**
 * A TypeFactory is responsible for creating TypeDescriptors of various types.
 * It also provides a mechanism for generating DataValue objects based on the
 * type information.
 * 
 * @author Dibyendu Majumdar
 */
public interface TypeFactory {

    /**
     * Creates a DataValue instance of the specified type.
     */
    DataValue getInstance(TypeDescriptor typeDesc);

    /**
     * Creates a DataValue instance of the specified type.
     */
    DataValue getInstance(TypeDescriptor typeDesc, ByteBuffer bb);

    /**
     * Returns a TypeDescriptor for the Varchar type.
     */
    TypeDescriptor getVarcharType(int maxLength);

    /**
     * Returns a TypeDescriptor for the Varbinary type.
     */
    TypeDescriptor getVarbinaryType(int maxLength);

    /**
     * Returns a TypeDescriptor for the Integer type.
     */
    TypeDescriptor getIntegerType();

    /**
     * Returns a TypeDescriptor for the Long type.
     */
    TypeDescriptor getLongType();

    /**
     * Returns a TypeDescriptor for the DateTime type.<br>
     * Timezone is defaulted to UTC.<br>
     * Format is defaulted to &quot;d-MMM-yyyy HH:mm:ss Z&quot;
     */
    TypeDescriptor getDateTimeType();

    /**
     * Returns a TypeDescriptor for the DateTime type.<br>
     * Format is defaulted to &quot;d-MMM-yyyy HH:mm:ss Z&quot;
     */
    TypeDescriptor getDateTimeType(String timezone);

    /**
     * Returns a TypeDescriptor for the DateTime type.<br>
     */
    TypeDescriptor getDateTimeType(String timezone, String format);

    /**
     * Returns a TypeDescriptor for the Numeric data type.<br>
     * Scale defaults to 0. Number types have following restrictions compared to
     * BigDecimals:
     * <ol>
     * <li>The maximum scale is limited to 127.</li>
     * <li>The maximum length of the number as a byte array is limited to 127.</li>
     * </ol>
     * Above limitations are simply to save storage space when persisting Number
     * objects.
     */
    TypeDescriptor getNumberType();

    /**
     * Returns a TypeDescriptor for the Numeric data type.
     */
    TypeDescriptor getNumberType(int scale);

    /**
     * Retrieves an array of data TypeDescriptors from a buffer stream.
     */
    TypeDescriptor[] retrieve(ByteBuffer bb);

    /**
     * Persists an array of data TypeDescriptors to a buffer stream.
     */
    void store(TypeDescriptor[] rowType, ByteBuffer bb);

    /**
     * Calculates the persisted length of an array of TypeDescriptor objects.
     */
    int getStoredLength(TypeDescriptor[] rowType);

}
