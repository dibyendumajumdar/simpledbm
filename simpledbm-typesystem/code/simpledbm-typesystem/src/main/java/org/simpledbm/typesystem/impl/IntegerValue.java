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
import org.simpledbm.typesystem.api.TypeDescriptor;

public class IntegerValue extends BaseDataValue {

	int i;
	
	public IntegerValue(TypeDescriptor typeDesc) {
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
    		n += TypeSize.INTEGER;
    	}
    	return n;
    }
    
    @Override
    public void store(ByteBuffer bb) {
    	super.store(bb);
    	if (isValue()) {
    		bb.putInt(i);
    	}
    }
    
    @Override
    public void retrieve(ByteBuffer bb) {
    	super.retrieve(bb);
    	if (isValue()) {
    		i = bb.getInt();
    	}
    }

	@Override
	public int getInt() {
		if (!isValue()) {
			return 0;
		}
		return i;
	}

	@Override
	public String getString() {
    	if (isValue()) {
    		return Integer.toString(i);
    	}
    	return super.toString();
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

	protected int compare(IntegerValue o) {
		int comp = super.compare(o);
		if (comp != 0 || !isValue()) {
			return comp;
		}
		return i - o.i;
		
	}
	
	@Override
    public int compareTo(DataValue o) {
    	if (this == o) {
    		return 0;
    	}
    	if (o == null) {
    		throw new IllegalArgumentException();
    	}
    	if (!(o instanceof IntegerValue)) {
    		throw new ClassCastException("Cannot cast " + o.getClass() + " to " + this.getClass());
    	}
		return compare((IntegerValue)o);
	}
	
    @Override
    public boolean equals(Object o) {
    	if (this == o) {
    		return true;
    	}
    	if (o == null) {
    		throw new IllegalArgumentException();
    	}
    	if (!(o instanceof IntegerValue)) {
    		throw new ClassCastException("Cannot cast " + o.getClass() + " to " + this.getClass());
    	}
		return compare((IntegerValue)o) == 0;
    }

	@Override
	public Object clone() throws CloneNotSupportedException {
		IntegerValue o = (IntegerValue) super.clone();
		return o;
	}
	
	@Override
	public StringBuilder appendTo(StringBuilder sb) {
		if (isValue()) {
			return sb.append(i);
		}
		return super.appendTo(sb);
	}
	
}

