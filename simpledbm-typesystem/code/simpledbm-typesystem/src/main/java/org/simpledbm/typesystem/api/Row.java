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
 *    Linking this library statically or dynamically with other modules 
 *    is making a combined work based on this library. Thus, the terms and
 *    conditions of the GNU General Public License cover the whole
 *    combination.
 *    
 *    As a special exception, the copyright holders of this library give 
 *    you permission to link this library with independent modules to 
 *    produce an executable, regardless of the license terms of these 
 *    independent modules, and to copy and distribute the resulting 
 *    executable under terms of your choice, provided that you also meet, 
 *    for each linked independent module, the terms and conditions of the 
 *    license of that module.  An independent module is a module which 
 *    is not derived from or based on this library.  If you modify this 
 *    library, you may extend this exception to your version of the 
 *    library, but you are not obligated to do so.  If you do not wish 
 *    to do so, delete this exception statement from your version.
 *
 *    Project: www.simpledbm.org
 *    Author : Dibyendu Majumdar
 *    Email  : d dot majumdar at gmail dot com ignore
 */
package org.simpledbm.typesystem.api;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import org.simpledbm.rss.api.im.IndexKey;
import org.simpledbm.rss.util.Dumpable;

/**
 * A Row is an array of DataValue objects, and can be used as multi-value record within
 * a table or an index.
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
	 * @throws UnsupportedOperationException.
	 */
	int getInt(int column);

	/**
	 * Converts the supplied integer value to a suitable value for the data value 
	 * @throws UnsupportedOperationException.
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
	 * @throws UnsupportedOperationException.
	 */
	void setDate(int column, Date date);
	
	/**
	 * Gets the current value as a date.
	 * @throws UnsupportedOperationException.
	 */
	Date getDate(int column);
	
	/**
	 * Converts the supplied long value to the data value
	 * @throws UnsupportedOperationException.
	 */
	void setLong(int column, long l);
	
	/**
	 * Returns the data value as a long
	 * @throws UnsupportedOperationException.
	 */
	long getLong(int column);
	
	/**
	 * Converts the supplied BigInteger to the data value
	 * @throws UnsupportedOperationException.
	 */
	void setBigInteger(int column, BigInteger i);
	
	/**
	 * Returns the data value as a long
	 * @throws UnsupportedOperationException.
	 */
	BigInteger getBigInteger(int column);
	
	/**
	 * Converts the supplied BigDecimal to the data value
	 * @throws UnsupportedOperationException.
	 */
	void setBigDecimal(int column, BigDecimal d);
	
	/**
	 * Returns the Data Value as a BigDecimal
	 * @throws UnsupportedOperationException.
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
	 * Checks if the data value is NULL, which is a special value indicating that the field's value
	 * has not been set.
	 */
	boolean isNull(int column);
	
	/**
	 * Sets the field to be NULL.
	 */
    void setNull(int column);

    /**
     * Checks if this field is set to the value signifying negative infinity.
     * Negative infinity represents a value that is less than all the valid values
     * in the field's domain.
     */
    boolean isNegativeInfinity(int column);

    /**
     * Sets this field to the value of negative infinity.
     */
    void setNegativeInfinity(int column);
    
    /**
     * Checks if this field is set to the value signifying positive infinity.
     * Positive infinity represents a value that is greater than all the valid values
     * in the field's domain.
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
