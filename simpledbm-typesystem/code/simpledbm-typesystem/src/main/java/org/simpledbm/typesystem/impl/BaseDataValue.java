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
/*
 * Created on: Nov 14, 2005
 * Author: Dibyendu Majumdar
 */
package org.simpledbm.typesystem.impl;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Date;

import org.simpledbm.common.util.TypeSize;
import org.simpledbm.typesystem.api.DataValue;
import org.simpledbm.typesystem.api.TypeDescriptor;

abstract class BaseDataValue implements DataValue {

    private static final int NULL_FIELD = 1;
    private static final int MINUS_INFINITY_FIELD = 2;
    private static final int VALUE_FIELD = 4;
    private static final int PLUS_INFINITY_FIELD = 8;
    
    private static final String NULL_VALUE = "null";
    private static final String MAX_VALUE = "+infinity";
    private static final String MIN_VALUE = "-infinity";
    
    private byte statusByte = 0; 
    private final TypeDescriptor typeDesc;
    
    protected BaseDataValue(BaseDataValue other) {
    	this.statusByte = other.statusByte;
    	this.typeDesc = other.typeDesc;
    }
    
    protected BaseDataValue(TypeDescriptor typeDesc) {
        statusByte = NULL_FIELD;
        this.typeDesc = typeDesc;
    }
    
    protected BaseDataValue(TypeDescriptor typeDesc, ByteBuffer bb) {
    	this.typeDesc = typeDesc;
        statusByte = bb.get();
    }
    
    public int getInt() {
        throw new UnsupportedOperationException();
    }

    public void setInt(Integer integer) {
        throw new UnsupportedOperationException();
    }

    public String getString() {
        throw new UnsupportedOperationException();
    }

    public void setString(String string) {
        throw new UnsupportedOperationException();
    }
    
    public BigDecimal getBigDecimal() {
        throw new UnsupportedOperationException();
	}

	public BigInteger getBigInteger() {
        throw new UnsupportedOperationException();
	}

	public Date getDate() {
        throw new UnsupportedOperationException();
	}

	public long getLong() {
        throw new UnsupportedOperationException();
	}

	public void setBigDecimal(BigDecimal d) {
        throw new UnsupportedOperationException();
	}

	public void setBigInteger(BigInteger i) {
        throw new UnsupportedOperationException();
	}

	public void setDate(Date date) {
        throw new UnsupportedOperationException();
	}

	public void setLong(long l) {
        throw new UnsupportedOperationException();
	}
	
	public byte[] getBytes() {
        throw new UnsupportedOperationException();
	}

	public void setBytes(byte[] bytes) {
        throw new UnsupportedOperationException();
	}

//	public void retrieve(ByteBuffer bb) {
//        statusByte = bb.get();
//    }

    public void store(ByteBuffer bb) {
        bb.put(statusByte);
    }

    public int getStoredLength() {
        return TypeSize.BYTE;
    }

    protected int compare(BaseDataValue o) {
        return statusByte - o.statusByte;
    }
    
    public int compareTo(DataValue other) {
    	if (this == other) {
    		return 0;
    	}
    	if (other == null) {
    		throw new IllegalArgumentException();
    	}
    	if (!(other instanceof BaseDataValue)) {
    		throw new ClassCastException("Cannot cast " + other.getClass() + " to " + this.getClass());
    	}
        return compare((BaseDataValue)other);
    }

    public final boolean isNull() {
        return (statusByte & NULL_FIELD) != 0;
    }

    public final void setNull() {
        statusByte = NULL_FIELD;
    }

    public final boolean isNegativeInfinity() {
        return (statusByte & MINUS_INFINITY_FIELD) != 0;
    }

    public final boolean isPositiveInfinity() {
        return (statusByte & PLUS_INFINITY_FIELD) != 0;
    }

    public final void setNegativeInfinity() {
        statusByte = MINUS_INFINITY_FIELD;
    }

    public final void setPositiveInfinity() {
        statusByte = PLUS_INFINITY_FIELD;
    }

    public final boolean isValue() {
        return (statusByte & VALUE_FIELD) != 0;
    }

    protected final void setValue() {
        statusByte = VALUE_FIELD;
    }

	@Override
    public boolean equals(Object o) {
    	if (this == o) {
    		return true;
    	}
    	if (o == null) {
    		throw new IllegalArgumentException();
    	}
    	if (!(o instanceof BaseDataValue)) {
    		throw new ClassCastException("Cannot cast " + o.getClass() + " to " + this.getClass());
    	}
        return compare((BaseDataValue) o) == 0;
    }

    public final TypeDescriptor getType() {
        return typeDesc;
    }
    
    public String toString() {
    	if (isNull()) {
    		return NULL_VALUE;
    	}
    	if (isPositiveInfinity()) {
    		return MAX_VALUE;
    	}
    	if (isNegativeInfinity()) {
    		return MIN_VALUE;
    	}
    	return "";
    }

	public StringBuilder appendTo(StringBuilder sb) {
    	if (isNull()) {
    		return sb.append(NULL_VALUE);
    	}
    	if (isPositiveInfinity()) {
    		return sb.append(MAX_VALUE);
    	}
    	if (isNegativeInfinity()) {
    		return sb.append(MIN_VALUE);
    	}
		return sb;
	}

    
}
