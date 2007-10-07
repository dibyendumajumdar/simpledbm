package org.simpledbm.typesystem.impl;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.simpledbm.typesystem.api.TypeDescriptor;

public class DateTimeType implements TypeDescriptor {

	final TimeZone timeZone;
	final DateFormat dateFormat;
	
	public DateTimeType(String timeZone, String format) {
		this.timeZone = TimeZone.getTimeZone(timeZone);
		this.dateFormat = new SimpleDateFormat(format);
		dateFormat.setTimeZone(this.timeZone);
	}
	
	public int getMaxLength() {
		return -1;
	}

	public int getScale() {
		return -1;
	}

	public int getTypeCode() {
		return TYPE_DATETIME;
	}

	public TimeZone getTimeZone() {
		return timeZone;
	}

	public DateFormat getDateFormat() {
		return dateFormat;
	}

}
