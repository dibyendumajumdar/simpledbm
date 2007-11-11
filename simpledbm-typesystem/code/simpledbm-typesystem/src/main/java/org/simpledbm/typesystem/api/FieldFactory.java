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
 *    Email  : dibyendu@mazumdar.demon.co.uk
 */
package org.simpledbm.typesystem.api;

import java.nio.ByteBuffer;

/**
 * A FieldFactory is responsible for creating fields of specified type.
 * 
 * @author Dibyendu Majumdar
 */
public interface FieldFactory {
	
	/**
	 * Creates a field instance of the specified type.
	 */
	Field getInstance(TypeDescriptor typeDesc);
	
 	TypeDescriptor getVarcharType(int maxLength);
	
	TypeDescriptor getIntegerType();
	
	TypeDescriptor getDateTimeType();
	
	TypeDescriptor getDateTimeType(String timezone);
	
	TypeDescriptor getDateTimeType(String timezone, String format);
	
	TypeDescriptor getNumberType();
	
	TypeDescriptor getNumberType(int scale);
	
	TypeDescriptor[] retrieve(ByteBuffer bb);
	
	void store(TypeDescriptor[] rowType, ByteBuffer bb);
	
	int getStoredLength(TypeDescriptor[] rowType);
	
}
