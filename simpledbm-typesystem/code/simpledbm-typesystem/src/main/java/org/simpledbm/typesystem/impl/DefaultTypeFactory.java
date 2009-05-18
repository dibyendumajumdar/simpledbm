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
package org.simpledbm.typesystem.impl;

import java.nio.ByteBuffer;

import org.simpledbm.rss.util.TypeSize;
import org.simpledbm.typesystem.api.DataValue;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.api.TypeFactory;


public class DefaultTypeFactory implements TypeFactory {
	
	/*
	 * Cache some common types.
	 */
	static TypeDescriptor integerType = new IntegerType();
	static TypeDescriptor longType = new LongType();
	static TypeDescriptor numberType = new NumberType(0);

	public DataValue getInstance(TypeDescriptor typeDesc) {
        switch (typeDesc.getTypeCode()) {
        case TypeDescriptor.TYPE_VARCHAR: return new VarcharValue(typeDesc);
        case TypeDescriptor.TYPE_INTEGER: return new IntegerValue(typeDesc);
        case TypeDescriptor.TYPE_LONG_INTEGER: return new LongValue(typeDesc);
        case TypeDescriptor.TYPE_NUMBER: return new NumberValue(typeDesc);
        case TypeDescriptor.TYPE_DATETIME: return new DateTimeValue(typeDesc);
        case TypeDescriptor.TYPE_BINARY: return new VarbinaryValue(typeDesc);
        }
        throw new IllegalArgumentException("Unknown type: " + typeDesc);
    }

	public DataValue getInstance(TypeDescriptor typeDesc, ByteBuffer bb) {
        switch (typeDesc.getTypeCode()) {
        case TypeDescriptor.TYPE_VARCHAR: return new VarcharValue(typeDesc, bb);
        case TypeDescriptor.TYPE_INTEGER: return new IntegerValue(typeDesc, bb);
        case TypeDescriptor.TYPE_LONG_INTEGER: return new LongValue(typeDesc, bb);
        case TypeDescriptor.TYPE_NUMBER: return new NumberValue(typeDesc, bb);
        case TypeDescriptor.TYPE_DATETIME: return new DateTimeValue(typeDesc, bb);
        case TypeDescriptor.TYPE_BINARY: return new VarbinaryValue(typeDesc, bb);
        }
        throw new IllegalArgumentException("Unknown type: " + typeDesc);
    }	
	
	public TypeDescriptor getVarbinaryType(int maxLength) {
		return new VarbinaryType(maxLength);
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

	public TypeDescriptor getLongType() {
		return longType;
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
	
//	private TypeDescriptor getTypeDescriptor(int typecode) {
//        switch (typecode) {
//        case TypeDescriptor.TYPE_VARCHAR: return new VarcharType();
//        case TypeDescriptor.TYPE_INTEGER: return new IntegerType();
//        case TypeDescriptor.TYPE_LONG_INTEGER: return new LongType();
//        case TypeDescriptor.TYPE_NUMBER: return new NumberType();
//        case TypeDescriptor.TYPE_DATETIME: return new DateTimeType();
//        case TypeDescriptor.TYPE_BINARY: return new VarbinaryType();
//        }
//        throw new IllegalArgumentException("Unknown type: " + typecode);
//	}

	private TypeDescriptor getTypeDescriptor(int typecode, ByteBuffer bb) {
        switch (typecode) {
        case TypeDescriptor.TYPE_VARCHAR: return new VarcharType(bb);
        case TypeDescriptor.TYPE_INTEGER: return new IntegerType(bb);
        case TypeDescriptor.TYPE_LONG_INTEGER: return new LongType(bb);
        case TypeDescriptor.TYPE_NUMBER: return new NumberType(bb);
        case TypeDescriptor.TYPE_DATETIME: return new DateTimeType(bb);
        case TypeDescriptor.TYPE_BINARY: return new VarbinaryType(bb);
        }
        throw new IllegalArgumentException("Unknown type: " + typecode);
	}	
	
	public TypeDescriptor[] retrieve(ByteBuffer bb) {
		int numberOfFields = bb.getShort();
		TypeDescriptor[] rowType = new TypeDescriptor[numberOfFields];
		for (int i = 0; i < numberOfFields; i++) {
			int typecode = bb.get();
			TypeDescriptor fieldType = getTypeDescriptor(typecode, bb);
//			fieldType.retrieve(bb);
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
