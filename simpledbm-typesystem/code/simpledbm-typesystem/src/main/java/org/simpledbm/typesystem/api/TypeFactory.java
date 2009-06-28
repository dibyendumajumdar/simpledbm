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

import java.nio.ByteBuffer;

/**
 * A TypeFactory is responsible for creating TypeDescriptors of various types.
 * It also provides a mechanism for generating DataValue objects based on 
 * the type information.
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
	 * Returns a TypeDescriptor for the DateTime type.<br/>
	 * Timezone is defaulted to UTC.<br/>
	 * Format is defaulted to &quot;d-MMM-yyyy HH:mm:ss Z&quot;
	 */
	TypeDescriptor getDateTimeType();
	
	/**
	 * Returns a TypeDescriptor for the DateTime type.<br/>
	 * Format is defaulted to &quot;d-MMM-yyyy HH:mm:ss Z&quot;
	 */
	TypeDescriptor getDateTimeType(String timezone);
	
	/**
	 * Returns a TypeDescriptor for the DateTime type.<br/>
	 */
	TypeDescriptor getDateTimeType(String timezone, String format);
	
	/**
	 * Returns a TypeDescriptor for the Numeric data type.<br />
	 * Scale defaults to 0.
	 * Number types have following restrictions compared to BigDecimals:
	 * <ol>
	 * <li>The maximum scale is limited to 127.</li>
	 * <li>The maximum length of the number as a byte array is limited to 127.</li>
	 * </ol>
	 * Above limitations are simply to save storage space when persisting
	 * Number objects. 
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
