package org.simpledbm.typesystem.impl;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.util.TimeZone;

import org.simpledbm.rss.util.TypeSize;
import org.simpledbm.typesystem.api.TypeDescriptor;

public class NumberType implements TypeDescriptor {

	int scale;
	
	NumberType() {
	}
	
	public NumberType(int scale) {
		this.scale = scale;
	}
	
	public int getMaxLength() {
		return -1;
	}

	public int getScale() {
		return scale;
	}

	public int getTypeCode() {
		return TYPE_NUMBER;
	}

	public boolean isFixedLength() {
		return false;
	}

	public DateFormat getDateFormat() {
		return null;
	}

	public TimeZone getTimeZone() {
		return null;
	}

	public int getStoredLength() {
		return TypeSize.INTEGER;
	}

	public void retrieve(ByteBuffer bb) {
		scale = bb.getInt();
	}

	public void store(ByteBuffer bb) {
		bb.putInt(scale);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + scale;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final NumberType other = (NumberType) obj;
		if (scale != other.scale)
			return false;
		return true;
	}
	
	

}
