/***
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program; if not, write to the Free Software
 *    Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *
 *    Project: www.simpledbm.org
 *    Author : Dibyendu Majumdar
 *    Email  : d dot majumdar at gmail dot com ignore
 */
/*
 * Created on: Nov 14, 2005
 * Author: Dibyendu Majumdar
 */
package org.simpledbm.typesystem.api;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import org.simpledbm.rss.api.st.Storable;

/**
 * A DataValue is a data item with a type, and suitable for use in a multi-column row.
 * The set of values that a DataValue can hold is called its value domain. The value domain is logically
 * bounded by "negative infinity" and "positive infinity", such that:
 * <p>
 * <pre>
 * null &lt; negative_infinity &lt; value &lt; positive_infinity
 * </pre>
 * 
 * @author Dibyendu Majumdar
 */
public interface DataValue extends Storable, Comparable<DataValue> {

	/**
	 * Returns an integer representation of the data value.
	 */
	int getInt();

	/**
	 * Converts the supplied integer value to a suitable value for the data value 
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
	 */
	void setDate(Date date);
	
	/**
	 * Gets the current value as a date.
	 */
	Date getDate();
	
	/**
	 * Converts the supplied long value to the data value
	 */
	void setLong(long l);
	
	/**
	 * Returns the data value as a long
	 */
	long getLong();
	
	/**
	 * Converts the supplied BigInteger to the data value
	 */
	void setBigInteger(BigInteger i);
	
	/**
	 * Returns the data value as a long
	 */
	BigInteger getBigInteger();
	
	/**
	 * Converts the supplied BigDecimal to the data value
	 */
	void setBigDecimal(BigDecimal d);
	
	/**
	 * Returns the Data Value as a BigDecimal
	 */
	BigDecimal getBigDecimal();
	
	/**
	 * Checks if the data value is NULL, which is a special value indicating that the field's value
	 * has not been set.
	 */
	boolean isNull();
	
	/**
	 * Sets the field to be NULL.
	 */
    void setNull();

    /**
     * Checks if this field is set to the value signifying negative infinity.
     * Negative infinity represents a value that is less than all the valid values
     * in the field's domain.
     */
    boolean isNegativeInfinity();

    /**
     * Sets this field to the value of negative infinity.
     */
    void setNegativeInfinity();
    
    /**
     * Checks if this field is set to the value signifying positive infinity.
     * Positive infinity represents a value that is greater than all the valid values
     * in the field's domain.
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
