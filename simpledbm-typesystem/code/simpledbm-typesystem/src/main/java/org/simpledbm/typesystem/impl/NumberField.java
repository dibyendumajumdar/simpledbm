package org.simpledbm.typesystem.impl;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.simpledbm.rss.util.TypeSize;
import org.simpledbm.typesystem.api.Field;
import org.simpledbm.typesystem.api.TypeDescriptor;

public class NumberField extends BaseField {

	BigDecimal d;
	
	protected NumberField(TypeDescriptor typeDesc) {
		super(typeDesc);
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		NumberField f = (NumberField) super.clone();
		f.d = new BigDecimal(d.unscaledValue(), d.scale());
		return f;
	}

	protected int compare(NumberField o) {
		int comp = super.compareTo(o);
		if (comp != 0 || !isValue()) {
			return comp;
		}
		return d.compareTo(o.d);	
	}	
	
	@Override
	public int compareTo(Field o) {
		if (o == this) {
			return 0;
		}
        if (o == null) {
        	throw new IllegalArgumentException();
        }
        if (!(o instanceof NumberField)) {
            throw new ClassCastException();
        }		
		return compare((NumberField)o);	
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
        if (o == null) {
        	throw new IllegalArgumentException();
        }
        if (!(o instanceof NumberField)) {
            throw new ClassCastException();
        }		
		return compare((NumberField)o) == 0;	
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
			byte[] data = i.toByteArray();
			n += TypeSize.BYTE * 2 + data.length;
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

	@Override
	public void retrieve(ByteBuffer bb) {
		super.retrieve(bb);
		if (isValue()) {
			int scale = bb.get();
			int len = bb.get();
			byte[] data = new byte[len];
			bb.get(data);
			d = new BigDecimal(new BigInteger(data), scale);
		}
	}

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



}
