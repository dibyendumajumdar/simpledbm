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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import org.simpledbm.common.api.key.IndexKey;
import org.simpledbm.common.util.Dumpable;

/**
 * A Row is an array of DataValue objects, and can be used as multi-value record
 * within a table or an index.
 * 
 * @author Dibyendu Majumdar
 */
public interface Row extends IndexKey, Dumpable {

    /**
     * Returns the number of fields in the row.
     */
    int getNumberOfColumns();

    /**
     * Creates a copy of this row.
     */
    Row cloneMe();

    /**
     * Returns an integer representation of the data value.
     * 
     * @throws UnsupportedOperationException If conversion is not supported for the type
     */
    int getInt(int column);

    /**
     * Converts the supplied integer value to a suitable value for the data
     * value
     *
     * @throws UnsupportedOperationException If conversion is not supported for the type
     */
    void setInt(int column, Integer integer);

    /**
     * Returns a string representation of the data value.
     */
    String getString(int column);

    /**
     * Converts the supplied string value to a suitable value for the data value
     */
    void setString(int column, String string);

    /**
     * Sets the value of the field to the date.
     *
     * @throws UnsupportedOperationException If conversion is not supported for the type
     */
    void setDate(int column, Date date);

    /**
     * Gets the current value as a date.
     *
     * @throws UnsupportedOperationException If conversion is not supported for the type
     */
    Date getDate(int column);

    /**
     * Converts the supplied long value to the data value
     *
     * @throws UnsupportedOperationException If conversion is not supported for the type
     */
    void setLong(int column, long l);

    /**
     * Returns the data value as a long
     *
     * @throws UnsupportedOperationException If conversion is not supported for the type
     */
    long getLong(int column);

    /**
     * Converts the supplied BigInteger to the data value
     *
     * @throws UnsupportedOperationException If conversion is not supported for the type
     */
    void setBigInteger(int column, BigInteger i);

    /**
     * Returns the data value as a long
     *
     * @throws UnsupportedOperationException If conversion is not supported for the type
     */
    BigInteger getBigInteger(int column);

    /**
     * Converts the supplied BigDecimal to the data value
     *
     * @throws UnsupportedOperationException If conversion is not supported for the type
     */
    void setBigDecimal(int column, BigDecimal d);

    /**
     * Returns the Data Value as a BigDecimal
     *
     * @throws UnsupportedOperationException If conversion is not supported for the type
     */
    BigDecimal getBigDecimal(int column);

    /**
     * Returns the DataValue as a byte array
     */
    byte[] getBytes(int column);

    /**
     * Sets the DataValue to the byte array
     */
    void setBytes(int column, byte[] bytes);

    /**
     * Checks if the data value is NULL, which is a special value indicating
     * that the field's value has not been set.
     */
    boolean isNull(int column);

    /**
     * Sets the field to be NULL.
     */
    void setNull(int column);

    /**
     * Checks if this field is set to the value signifying negative infinity.
     * Negative infinity represents a value that is less than all the valid
     * values in the field's domain.
     */
    boolean isNegativeInfinity(int column);

    /**
     * Sets this field to the value of negative infinity.
     */
    void setNegativeInfinity(int column);

    /**
     * Checks if this field is set to the value signifying positive infinity.
     * Positive infinity represents a value that is greater than all the valid
     * values in the field's domain.
     */
    boolean isPositiveInfinity(int column);

    /**
     * Sets this field to the value of positive infinity.
     */
    void setPositiveInfinity(int column);

    /**
     * Checks if this field is set to a value in the field's domain.
     */
    boolean isValue(int column);

    /**
     * Compare a column of this row with column in another row.
     */
    int compareTo(Row row, int column);

    /**
     * Copies the column by copying from another row.
     */
    void set(int column, Row sourceRow, int sourceColumn);
}
