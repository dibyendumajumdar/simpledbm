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
/*
 * Created on: Nov 14, 2005
 * Author: Dibyendu Majumdar
 */
package org.simpledbm.typesystem.api;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.util.Dumpable;

/**
 * A DataValue is a data item with a type, and suitable for use in a
 * multi-column row. The set of values that a DataValue can hold is called its
 * value domain. The value domain is logically bounded by "negative infinity"
 * and "positive infinity", such that:
 * <p>
 * 
 * <pre>
 * null &lt; negative_infinity &lt; value &lt; positive_infinity
 * </pre>
 * 
 * @author Dibyendu Majumdar
 */
public interface DataValue extends Storable, Comparable<DataValue>, Dumpable {

    /**
     * Returns an integer representation of the data value.
     *
     * @throws UnsupportedOperationException.
     */
    int getInt();

    /**
     * Converts the supplied integer value to a suitable value for the data
     * value
     *
     * @throws UnsupportedOperationException.
     */
    void setInt(Integer integer);

    /**
     * Returns a string representation of the data value.
     */
    String getString();

    /**
     * Converts the supplied string value to a suitable value for the data value
     */
    void setString(String string);

    /**
     * Sets the value of the field to the date.
     *
     * @throws UnsupportedOperationException.
     */
    void setDate(Date date);

    /**
     * Gets the current value as a date.
     *
     * @throws UnsupportedOperationException.
     */
    Date getDate();

    /**
     * Converts the supplied long value to the data value
     *
     * @throws UnsupportedOperationException.
     */
    void setLong(long l);

    /**
     * Returns the data value as a long
     *
     * @throws UnsupportedOperationException.
     */
    long getLong();

    /**
     * Converts the supplied BigInteger to the data value
     *
     * @throws UnsupportedOperationException.
     */
    void setBigInteger(BigInteger i);

    /**
     * Returns the data value as a long
     *
     * @throws UnsupportedOperationException.
     */
    BigInteger getBigInteger();

    /**
     * Converts the supplied BigDecimal to the data value
     * 
     * @throws UnsupportedOperationException.
     */
    void setBigDecimal(BigDecimal d);

    /**
     * Returns the Data Value as a BigDecimal
     * 
     * @throws UnsupportedOperationException.
     */
    BigDecimal getBigDecimal();

    /**
     * Returns the DataValue as a byte array
     */
    byte[] getBytes();

    /**
     * Sets the DataValue to the byte array
     */
    void setBytes(byte[] bytes);

    /**
     * Checks if the data value is NULL, which is a special value indicating
     * that the field's value has not been set.
     */
    boolean isNull();

    /**
     * Sets the field to be NULL.
     */
    void setNull();

    /**
     * Checks if this field is set to the value signifying negative infinity.
     * Negative infinity represents a value that is less than all the valid
     * values in the field's domain.
     */
    boolean isNegativeInfinity();

    /**
     * Sets this field to the value of negative infinity.
     */
    void setNegativeInfinity();

    /**
     * Checks if this field is set to the value signifying positive infinity.
     * Positive infinity represents a value that is greater than all the valid
     * values in the field's domain.
     */
    boolean isPositiveInfinity();

    /**
     * Sets this field to the value of positive infinity.
     */
    void setPositiveInfinity();

    /**
     * Checks if this field is set to a value in the field's domain.
     */
    boolean isValue();

    /**
     * Returns the TypeDescriptor for the field
     */
    TypeDescriptor getType();

    /**
     * Creates and returns a copy of this field.
     */
    DataValue cloneMe();
}
