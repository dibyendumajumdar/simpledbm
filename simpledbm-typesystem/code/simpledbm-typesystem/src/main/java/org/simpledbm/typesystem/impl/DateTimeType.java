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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.simpledbm.rss.util.ByteString;
import org.simpledbm.typesystem.api.TypeDescriptor;

public class DateTimeType implements TypeDescriptor {

	TimeZone timeZone;
	SimpleDateFormat dateFormat;
	
	DateTimeType() {
	}
	
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

	public int getStoredLength() {
		ByteString bs1 = new ByteString(timeZone.getID());
		ByteString bs2 = new ByteString(dateFormat.toPattern());
		return bs1.getStoredLength() + bs2.getStoredLength();
	}

	public void retrieve(ByteBuffer bb) {
		ByteString bs = new ByteString();
		bs.retrieve(bb);
		timeZone = TimeZone.getTimeZone(bs.toString());
		bs.retrieve(bb);
		dateFormat = new SimpleDateFormat(bs.toString());
		dateFormat.setTimeZone(timeZone);
	}

	public void store(ByteBuffer bb) {
		String s = timeZone.getID();
		ByteString bs = new ByteString(s);
		bs.store(bb);
		s = dateFormat.toPattern();
		bs = new ByteString(s);
		bs.store(bb);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((dateFormat == null) ? 0 : dateFormat.hashCode());
		result = prime * result
				+ ((timeZone == null) ? 0 : timeZone.getID().hashCode());
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
		final DateTimeType other = (DateTimeType) obj;
		if (dateFormat == null) {
			if (other.dateFormat != null)
				return false;
		} else if (!dateFormat.equals(other.dateFormat))
			return false;
		if (timeZone == null) {
			if (other.timeZone != null)
				return false;
		} else if (!timeZone.getID().equals(other.timeZone.getID()))
			return false;
		return true;
	}

	public StringBuilder appendTo(StringBuilder sb) {
		sb.append("DataTimeType(timeZone=").
			append(timeZone.getID()).
			append(", dateFormat='").
			append(dateFormat.toPattern()).
			append("')");
		return sb;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		return appendTo(sb).toString();
	}
}
