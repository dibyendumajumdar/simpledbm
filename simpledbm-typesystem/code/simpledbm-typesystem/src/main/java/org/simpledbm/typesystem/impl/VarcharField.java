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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.simpledbm.rss.util.TypeSize;
import org.simpledbm.typesystem.api.Field;
import org.simpledbm.typesystem.api.TypeDescriptor;

public class VarcharField extends BaseField {

	private char[] charArray;

	public VarcharField(TypeDescriptor typeDesc) {
		super(typeDesc);
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
    
    @Override
    public void retrieve(ByteBuffer bb) {
    	super.retrieve(bb);
    	if (isValue()) {
			short n = bb.getShort();
			charArray = new char[n];
			for (int i = 0; i < charArray.length; i++) {
				charArray[i] = bb.getChar();
			}
		}
    }

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

    protected int compare(VarcharField other) {
		int comp = super.compare(other);
		if (comp != 0 || !isValue()) {
			return comp;
		}
        VarcharField o = (VarcharField) other;
		int n1 = charArray == null ? 0 : charArray.length;
		int n2 = o.charArray == null ? 0 : o.charArray.length; 
		int prefixLen = Math.min(n1, n2);
		for (int i = 0; i < prefixLen; i++) {
			int rc = charArray[i] - o.charArray[i];
			if (rc != 0) {
				return rc;
			}
		}
		return n1 - n2;
	}
	
    @Override
    public int compareTo(Field other) {
    	if (other == this) {
    		return 0;
    	}
        if (other == null) {
        	throw new IllegalArgumentException();
        }
        if (!(other instanceof VarcharField)) {
            throw new ClassCastException();
        }
        return compare((VarcharField) other);
	}
	
	@Override
    public boolean equals(Object other) {
	   	if (other == this) {
    		return true;
    	}
        if (other == null) {
        	throw new IllegalArgumentException();
        }
        if (!(other instanceof VarcharField)) {
            throw new ClassCastException();
        }
        return compare((VarcharField) other) == 0;
    }

    public int length() {
    	if (isValue()) {
    		return charArray.length;
    	}
    	return 0;
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		VarcharField o = (VarcharField) super.clone();
		if (isValue()) {
			o.charArray = charArray.clone();
		}
		else {
			o.charArray = null;
		}
		return o;
	}

}
