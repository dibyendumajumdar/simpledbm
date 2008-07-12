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
import java.text.ParseException;
import java.util.Date;

import org.simpledbm.rss.util.TypeSize;
import org.simpledbm.typesystem.api.DataValue;
import org.simpledbm.typesystem.api.TypeDescriptor;

public class DateTimeValue extends BaseDataValue {

	long time;
	
	DateTimeValue(TypeDescriptor typeDesc) {
		super(typeDesc);
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}
	
	protected int compare(DateTimeValue o) {
		int comp = super.compare(o);
		if (comp != 0 || !isValue()) {
			return comp;
		}
		if (time == o.time) return 0;
		else if (time > o.time) return 1;
		else return -1;
		// return (int) (time - o.time);	
	}

	@Override
	public int compareTo(DataValue o) {
    	if (this == o) {
    		return 0;
    	}
    	if (o == null) {
    		throw new IllegalArgumentException();
    	}
    	if (!(o instanceof DateTimeValue)) {
    		throw new ClassCastException("Cannot cast " + o.getClass() + " to " + this.getClass());
    	}
    	return compare((DateTimeValue)o);
	}

	@Override
	public boolean equals(Object o) {
    	if (this == o) {
    		return true;
    	}
    	if (o == null) {
    		throw new IllegalArgumentException();
    	}
    	if (!(o instanceof DateTimeValue)) {
    		throw new ClassCastException("Cannot cast " + o.getClass() + " to " + this.getClass());
    	}        
    	return compare((DateTimeValue)o) == 0;
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
