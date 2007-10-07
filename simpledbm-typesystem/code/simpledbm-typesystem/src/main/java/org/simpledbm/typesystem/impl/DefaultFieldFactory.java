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
package org.simpledbm.typesystem.impl;

import org.simpledbm.typesystem.api.Field;
import org.simpledbm.typesystem.api.FieldFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;


public class DefaultFieldFactory implements FieldFactory {
	
	static TypeDescriptor integerType = new IntegerType();
	static TypeDescriptor numberType = new NumberType(0);

	public Field getInstance(TypeDescriptor typeDesc) {
        switch (typeDesc.getTypeCode()) {
        case TypeDescriptor.TYPE_VARCHAR: return new VarcharField(typeDesc);
        case TypeDescriptor.TYPE_INTEGER: return new IntegerField(typeDesc);
        case TypeDescriptor.TYPE_NUMBER: return new NumberField(typeDesc);
        case TypeDescriptor.TYPE_DATETIME: return new DateTimeField(typeDesc);
        }
        throw new IllegalArgumentException("Unknown type: " + typeDesc);
    }

	public TypeDescriptor getDateTimeType() {
		return new DateTimeType("UTC", "d-MMM-yyyy HH:mm:ss Z");
	}

	public TypeDescriptor getDateTimeType(String timezone, String format) {
		return new DateTimeType(timezone, format);
	}

	public TypeDescriptor getDateTimeType(String timezone) {
		return new DateTimeType(timezone, "d-MMM-yyyy HH:mm:ss Z");
	}

	public TypeDescriptor getIntegerType() {
		return integerType;
	}

	public TypeDescriptor getNumberType() {
		return numberType;
	}

	public TypeDescriptor getNumberType(int scale) {
		return new NumberType(scale);
	}

	public TypeDescriptor getVarcharType(int maxLength) {
		return new VarcharType(maxLength);
	}
   
	
}
