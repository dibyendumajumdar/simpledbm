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
package org.simpledbm.typesystem.impl;

import java.nio.ByteBuffer;

import org.simpledbm.rss.util.TypeSize;
import org.simpledbm.typesystem.api.DataValue;
import org.simpledbm.typesystem.api.TypeFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;


public class DefaultTypeFactory implements TypeFactory {
	
	static TypeDescriptor integerType = new IntegerType();
	static TypeDescriptor numberType = new NumberType(0);

	public DataValue getInstance(TypeDescriptor typeDesc) {
        switch (typeDesc.getTypeCode()) {
        case TypeDescriptor.TYPE_VARCHAR: return new VarcharValue(typeDesc);
        case TypeDescriptor.TYPE_INTEGER: return new IntegerValue(typeDesc);
        case TypeDescriptor.TYPE_NUMBER: return new NumberValue(typeDesc);
        case TypeDescriptor.TYPE_DATETIME: return new DateTimeValue(typeDesc);
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
	
	private TypeDescriptor getTypeDescriptor(int typecode) {
        switch (typecode) {
        case TypeDescriptor.TYPE_VARCHAR: return new VarcharType();
        case TypeDescriptor.TYPE_INTEGER: return new IntegerType();
        case TypeDescriptor.TYPE_NUMBER: return new NumberType();
        case TypeDescriptor.TYPE_DATETIME: return new DateTimeType();
        }
        throw new IllegalArgumentException("Unknown type: " + typecode);
	}

	public TypeDescriptor[] retrieve(ByteBuffer bb) {
		int numberOfFields = bb.getShort();
		TypeDescriptor[] rowType = new TypeDescriptor[numberOfFields];
		for (int i = 0; i < numberOfFields; i++) {
			int typecode = bb.get();
			TypeDescriptor fieldType = getTypeDescriptor(typecode);
			fieldType.retrieve(bb);
			rowType[i] = fieldType;
		}
		return rowType;
	}

	public void store(TypeDescriptor[] rowType, ByteBuffer bb) {
		bb.putShort((short) rowType.length);
		for (int i = 0; i < rowType.length; i++) {
			bb.put((byte) rowType[i].getTypeCode());
			rowType[i].store(bb);
		}
	}

	public int getStoredLength(TypeDescriptor[] rowType) {
		int len = TypeSize.SHORT + rowType.length * TypeSize.BYTE;
		for (int i = 0; i < rowType.length; i++) {
			len += rowType[i].getStoredLength();
		}
		return len;
	}
   
	
}
