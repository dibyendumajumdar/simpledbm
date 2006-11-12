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
        if (charArray == null)
            return "";
        return new String(charArray);
    }
    
    @Override
    public int getStoredLength() {
    	int n = super.getStoredLength() + TypeSize.SHORT;
    	if (charArray != null && !isNull()) {
    		n += charArray.length * TypeSize.CHARACTER;
    	}
        return n;
    }
    
    @Override
    public void store(ByteBuffer bb) {
    	super.store(bb);
        short n = 0;
        if (charArray != null && !isNull()) {
            n = (short) (charArray.length * TypeSize.CHARACTER);
        }
        bb.putShort(n);
        if (charArray != null && !isNull()) {
            for (char c: charArray) {
                bb.putChar(c);
            }
        }
    }
    
    @Override
    public void retrieve(ByteBuffer bb) {
    	super.retrieve(bb);
        short n = bb.getShort();
        if (n > 0) {
            charArray = new char[n / TypeSize.CHARACTER];
            for (int i = 0; i < charArray.length; i++) {
            	charArray[i] = bb.getChar();
            }
        }
        else {
            charArray = null;
        }
    }

	@Override
	public int getInt() {
		if (isNull() || charArray == null) {
			return 0;
		}
		return Integer.parseInt(new String(charArray));
	}

	@Override
	public String getString() {
		if (isNull() || charArray == null) {
			return "";
		}
		return new String(charArray);
	}

	@Override
	public void setInt(Integer integer) {
		setString(integer.toString());
	}

	@Override
	public void setString(String string) {
        if ((string.length()*TypeSize.CHARACTER) > getType().getMaxLength()) {
            string = string.substring(0, getType().getMaxLength()/TypeSize.CHARACTER);
        }
		charArray = string.toCharArray();
		setValue();
	}

    
    @Override
    public int compareTo(Field other) {
        if (other == null || !(other instanceof VarcharField)) {
            return -1;
        }
		int comp = super.compareTo(other);
		if (comp != 0) {
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
    public boolean equals(Object o) {
        if (o == null || !(o instanceof VarcharField)) {
            return false;
        }
        return compareTo((Field) o) == 0;
    }

    public int length() {
		if (isNull() || charArray == null) {
			return 0;
		}
		return charArray.length;
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		VarcharField o = (VarcharField) super.clone();
		if (!isNull() && charArray != null) {
			o.charArray = charArray.clone();
		}
		return o;
	}

}
