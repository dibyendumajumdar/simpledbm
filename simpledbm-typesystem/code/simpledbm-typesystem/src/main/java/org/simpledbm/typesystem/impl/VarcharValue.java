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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.simpledbm.common.util.TypeSize;
import org.simpledbm.typesystem.api.DataValue;
import org.simpledbm.typesystem.api.TypeDescriptor;

public class VarcharValue extends BaseDataValue {

	private char[] charArray;

	VarcharValue(VarcharValue other) {
		super(other);
		if (isValue()) {
			this.charArray = other.charArray.clone();
		}
		else {
			this.charArray = null;
		}
	}
	
	public VarcharValue(TypeDescriptor typeDesc) {
		super(typeDesc);
	}

	public VarcharValue(TypeDescriptor typeDesc, ByteBuffer bb) {
		super(typeDesc, bb);
    	if (isValue()) {
			short n = bb.getShort();
			charArray = new char[n];
			for (int i = 0; i < charArray.length; i++) {
				charArray[i] = bb.getChar();
			}
		}
	}	
	
    @Override
	public String toString() {
    	return getString();
    }
    
    @Override
    public int getStoredLength() {
    	int n = super.getStoredLength();
    	if (isValue()) {
    		n += TypeSize.SHORT;
    		n += charArray.length * TypeSize.CHARACTER;
    	}
        return n;
    }
    
    @Override
    public void store(ByteBuffer bb) {
    	super.store(bb);
        short n = 0;
        if (isValue()) {
			n = (short) charArray.length;
			bb.putShort(n);
			for (char c : charArray) {
				bb.putChar(c);
			}
		}
    }
    
//    @Override
//    public void retrieve(ByteBuffer bb) {
//    	super.retrieve(bb);
//    	if (isValue()) {
//			short n = bb.getShort();
//			charArray = new char[n];
//			for (int i = 0; i < charArray.length; i++) {
//				charArray[i] = bb.getChar();
//			}
//		}
//    }

	@Override
	public int getInt() {
		if (!isValue()) {
			return 0;
		}
		return Integer.parseInt(toString());
	}

	@Override
	public String getString() {
    	if (isValue()) {
    		return new String(charArray);
    	}
    	return super.toString();
	}

	@Override
	public void setInt(Integer integer) {
		setString(integer.toString());
	}

	@Override
	public BigDecimal getBigDecimal() {
		if (!isValue()) {
			return null;
		}
		BigDecimal d = new BigDecimal(charArray);
		return d;
	}

	@Override
	public BigInteger getBigInteger() {
		if (!isValue()) {
			return null;
		}
		BigInteger i = new BigInteger(toString());
		return i;
	}

	@Override
	public long getLong() {
		if (!isValue()) {
			return 0;
		}
		return Long.valueOf(toString());
	}

	@Override
	public void setBigDecimal(BigDecimal d) {
		setString(d.toString());
	}

	@Override
	public void setBigInteger(BigInteger i) {
		setString(i.toString());
	}

	@Override
	public void setLong(long l) {
		setString(Long.toString(l));
	}

	@Override
	public void setString(String string) {
        if (string.length() > getType().getMaxLength()) {
            string = string.substring(0, getType().getMaxLength());
        }
		charArray = string.toCharArray();
		setValue();
	}

    protected int compare(VarcharValue other) {
		int comp = super.compare(other);
		if (comp != 0 || !isValue()) {
			return comp;
		}
        VarcharValue o = (VarcharValue) other;
		int len1 = charArray == null ? 0 : charArray.length;
		int len2 = o.charArray == null ? 0 : o.charArray.length; 
		int prefixLen = Math.min(len1, len2);
		for (int i = 0; i < prefixLen; i++) {
			int c1 = charArray[i];
			int c2 = o.charArray[i];
			if (c1 != c2) {
				return c1 - c2;
			}
		}
		return len1 - len2;
	}
	
    @Override
    public int compareTo(DataValue other) {
    	if (other == this) {
    		return 0;
    	}
        if (other == null) {
        	throw new IllegalArgumentException();
        }
        if (!(other instanceof VarcharValue)) {
            throw new ClassCastException();
        }
        return compare((VarcharValue) other);
	}
	
	@Override
    public boolean equals(Object other) {
	   	if (other == this) {
    		return true;
    	}
        if (other == null) {
        	throw new IllegalArgumentException();
        }
        if (!(other instanceof VarcharValue)) {
            throw new ClassCastException();
        }
        return compare((VarcharValue) other) == 0;
    }

    public int length() {
    	if (isValue()) {
    		return charArray.length;
    	}
    	return 0;
	}

	public DataValue cloneMe() {
		return new VarcharValue(this);
	}

	@Override
	public StringBuilder appendTo(StringBuilder sb) {
		if (isValue()) {
			return sb.append(getString());
		}
		return super.appendTo(sb);
	}
}
