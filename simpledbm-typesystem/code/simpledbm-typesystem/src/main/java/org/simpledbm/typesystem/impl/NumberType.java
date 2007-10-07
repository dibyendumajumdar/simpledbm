package org.simpledbm.typesystem.impl;

import java.text.DateFormat;
import java.util.TimeZone;

import org.simpledbm.typesystem.api.TypeDescriptor;

public class NumberType implements TypeDescriptor {

	final int scale;
	
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

}
