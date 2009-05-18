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

import org.simpledbm.rss.util.TypeSize;
import org.simpledbm.typesystem.api.DataValue;
import org.simpledbm.typesystem.api.TypeDescriptor;

public class NumberValue extends BaseDataValue {

	BigDecimal d;
	
	public NumberValue(TypeDescriptor typeDesc) {
		super(typeDesc);
	}

	public NumberValue(NumberValue other) {
		super(other);
		this.d = new BigDecimal(other.d.unscaledValue(), other.d.scale());
	}
	
	public NumberValue(TypeDescriptor typeDesc, ByteBuffer bb) {
		super(typeDesc, bb);
		if (isValue()) {
			int scale = bb.get();
			int len = bb.get();
			byte[] data = new byte[len];
			bb.get(data);
			d = new BigDecimal(new BigInteger(data), scale);
		}
	}
	
	public DataValue cloneMe() {
		return new NumberValue(this);
	}

	protected int compare(NumberValue o) {
		int comp = super.compareTo(o);
		if (comp != 0 || !isValue()) {
			return comp;
		}
		return d.compareTo(o.d);	
	}	
	
	@Override
	public int compareTo(DataValue o) {
		if (o == this) {
			return 0;
		}
        if (o == null) {
        	throw new IllegalArgumentException();
        }
        if (!(o instanceof NumberValue)) {
            throw new ClassCastException();
        }		
		return compare((NumberValue)o);	
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
        if (o == null) {
        	throw new IllegalArgumentException();
        }
        if (!(o instanceof NumberValue)) {
            throw new ClassCastException();
        }		
		return compare((NumberValue)o) == 0;	
	}

	@Override
	public int getInt() {
		if (isValue()) {
			return d.intValue();
		}
		return 0;
	}

	@Override
	public int getStoredLength() {
		int n = super.getStoredLength();
		if (isValue()) {
			BigInteger i = d.unscaledValue();
			/* 
			 * Following length calculation is based
			 * upon logic in BigInteger class (openjdk).
			 */
	        int byteLen = i.bitLength()/8 + 1;
			//byte[] data = i.toByteArray();
	        //int byteLen = data.length;
			n += TypeSize.BYTE * 2 + byteLen;
		}
		return n;
	}

	@Override
	public String getString() {
		if (!isValue()) {
			return super.toString();
		}
		return d.toString();
	}

//	@Override
//	public void retrieve(ByteBuffer bb) {
//		super.retrieve(bb);
//		if (isValue()) {
//			int scale = bb.get();
//			int len = bb.get();
//			byte[] data = new byte[len];
//			bb.get(data);
//			d = new BigDecimal(new BigInteger(data), scale);
//		}
//	}

	@Override
	public void setInt(Integer integer) {
		d = new BigDecimal(integer);
		d = d.setScale(getType().getScale(), BigDecimal.ROUND_HALF_UP);
		setValue();
	}

	@Override
	public void setString(String string) {
		d = new BigDecimal(string);
		d = d.setScale(getType().getScale(), BigDecimal.ROUND_HALF_UP);
		setValue();
	}

	@Override
	public void store(ByteBuffer bb) {
		super.store(bb);
		if (isValue()) {
			byte[] data = d.unscaledValue().toByteArray();
			bb.put((byte) getType().getScale());
			bb.put((byte) data.length);
			bb.put(data);
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((!isValue()) ? 0 : d.hashCode());
		return result;
	}

	@Override
	public String toString() {
		return getString();
	}

	@Override
	public StringBuilder appendTo(StringBuilder sb) {
		if (isValue()) {
			return sb.append(getString());
		}
		return super.appendTo(sb);
	}
}
