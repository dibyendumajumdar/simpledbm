package org.simpledbm.typesystem.impl;

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.Date;

import org.simpledbm.rss.util.TypeSize;
import org.simpledbm.typesystem.api.Field;
import org.simpledbm.typesystem.api.TypeDescriptor;

public class DateTimeField extends BaseField {

	long time;
	
	DateTimeField(TypeDescriptor typeDesc) {
		super(typeDesc);
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}
	
	protected int compare(DateTimeField o) {
		int comp = super.compare(o);
		if (comp != 0 || !isValue()) {
			return comp;
		}
		return (int) (time - o.time);	
	}

	@Override
	public int compareTo(Field o) {
    	if (this == o) {
    		return 0;
    	}
    	if (o == null) {
    		throw new IllegalArgumentException();
    	}
    	if (!(o instanceof DateTimeField)) {
    		throw new ClassCastException("Cannot cast " + o.getClass() + " to " + this.getClass());
    	}
    	return compare((DateTimeField)o);
	}

	@Override
	public boolean equals(Object o) {
    	if (this == o) {
    		return true;
    	}
    	if (o == null) {
    		throw new IllegalArgumentException();
    	}
    	if (!(o instanceof DateTimeField)) {
    		throw new ClassCastException("Cannot cast " + o.getClass() + " to " + this.getClass());
    	}        
    	return compare((DateTimeField)o) == 0;
	}

	@Override
	public int getStoredLength() {
		int n = super.getStoredLength();
		if (isValue()) {
			n += TypeSize.LONG;
		}
		return n;
	}

	@Override
	public String getString() {
		if (isValue()) {
			return getMyType().getDateFormat().format(new Date(time));
		}
		return super.toString();
	}

	@Override
	public void retrieve(ByteBuffer bb) {
		super.retrieve(bb);
		if (isValue()) {
			time = bb.getLong();
		}
	}

	@Override
	public void setString(String string) {
		try {
			Date d = getMyType().getDateFormat().parse(string);
			time = d.getTime();
			setValue();
		} catch (ParseException e) {
			setNull();
			throw new IllegalArgumentException("Failed to parse : " + string, e);
		}
	}
	
	public void setDate(Date d) {
		this.time = d.getTime();
		setValue();
	}
	
	public Date getDate() {
		if (!isValue()) {
			return null;
		}
		return new Date(time);
	}

	public void setLong(long t) {
		Date d = new Date(t);
		setDate(d);
	}
	
	public long getLong() {
		if (!isValue()) {
			return 0;
		}
		return time;
	}
	
	@Override
	public void store(ByteBuffer bb) {
		super.store(bb);
		if (isValue()) {
			bb.putLong(time);
		}
	}

	@Override
	public String toString() {
		return getString();
	}
	
	private DateTimeType getMyType() {
		return (DateTimeType) getType();
	}
	
}
