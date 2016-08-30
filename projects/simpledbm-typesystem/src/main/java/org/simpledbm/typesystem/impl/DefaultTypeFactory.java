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
package org.simpledbm.typesystem.impl;

import java.nio.ByteBuffer;

import org.simpledbm.common.util.TypeSize;
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
        case TypeDescriptor.TYPE_VARCHAR:
            return new VarcharValue(typeDesc);
        case TypeDescriptor.TYPE_INTEGER:
            return new IntegerValue(typeDesc);
        case TypeDescriptor.TYPE_LONG_INTEGER:
            return new LongValue(typeDesc);
        case TypeDescriptor.TYPE_NUMBER:
            return new NumberValue(typeDesc);
        case TypeDescriptor.TYPE_DATETIME:
            return new DateTimeValue(typeDesc);
        case TypeDescriptor.TYPE_BINARY:
            return new VarbinaryValue(typeDesc);
        }
        throw new IllegalArgumentException("Unknown type: " + typeDesc);
    }

    public DataValue getInstance(TypeDescriptor typeDesc, ByteBuffer bb) {
        switch (typeDesc.getTypeCode()) {
        case TypeDescriptor.TYPE_VARCHAR:
            return new VarcharValue(typeDesc, bb);
        case TypeDescriptor.TYPE_INTEGER:
            return new IntegerValue(typeDesc, bb);
        case TypeDescriptor.TYPE_LONG_INTEGER:
            return new LongValue(typeDesc, bb);
        case TypeDescriptor.TYPE_NUMBER:
            return new NumberValue(typeDesc, bb);
        case TypeDescriptor.TYPE_DATETIME:
            return new DateTimeValue(typeDesc, bb);
        case TypeDescriptor.TYPE_BINARY:
            return new VarbinaryValue(typeDesc, bb);
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
        case TypeDescriptor.TYPE_VARCHAR:
            return new VarcharType(bb);
        case TypeDescriptor.TYPE_INTEGER:
            return new IntegerType(bb);
        case TypeDescriptor.TYPE_LONG_INTEGER:
            return new LongType(bb);
        case TypeDescriptor.TYPE_NUMBER:
            return new NumberType(bb);
        case TypeDescriptor.TYPE_DATETIME:
            return new DateTimeType(bb);
        case TypeDescriptor.TYPE_BINARY:
            return new VarbinaryType(bb);
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
