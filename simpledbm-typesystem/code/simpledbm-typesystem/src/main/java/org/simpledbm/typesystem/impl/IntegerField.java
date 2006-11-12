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

public class IntegerField extends BaseField {

	int i;
	
	public IntegerField(TypeDescriptor typeDesc) {
		super(typeDesc);
	}

    @Override
	public String toString() {
    	if (isNull()) {
    		return "null";
    	}
    	if (isPositiveInfinity()) {
    		return "+infinity";
    	}
    	if (isNegativeInfinity()) {
    		return "-infinity";
    	}
    	return Integer.toString(i);
    }
    
    @Override
    public int getStoredLength() {
    	return super.getStoredLength() + TypeSize.INTEGER;
    }
    
    @Override
    public void store(ByteBuffer bb) {
    	super.store(bb);
    	bb.putInt(i);
    }
    
    @Override
    public void retrieve(ByteBuffer bb) {
    	super.retrieve(bb);
    	i = bb.getInt();
    }

	@Override
	public int getInt() {
		if (isNull()) {
			return 0;
		}
		return i;
	}

	@Override
	public String getString() {
		if (isNull()) {
			return "0";
		}
        return Integer.toString(i);
	}

	@Override
	public void setInt(Integer integer) {
		i = integer;
		setValue();
	}

	@Override
	public void setString(String string) {
		setInt(Integer.parseInt(string));
	}

	@Override
    public int compareTo(Field o) {
        if (o == null || !(o instanceof IntegerField)) {
            return -1;
        }
		int comp = super.compareTo(o);
		if (comp != 0) {
			return comp;
		}
		return i - ((IntegerField)o).i;
	}
	
    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof IntegerField)) {
            return false;
        }
        return compareTo((Field) o) == 0;
    }
    
	public int length() {
		return getStoredLength();
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		IntegerField o = (IntegerField) super.clone();
		o.i = i;
		return o;
	}
	
}

