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
/*
 * Created on: Nov 16, 2005
 * Author: Dibyendu Majumdar
 */
package org.simpledbm.typesystem.impl;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.util.TimeZone;

import org.simpledbm.typesystem.api.TypeDescriptor;

/**
 * The IntegerType provides storage for a 32-bit integer.
 * @author Dibyendu Majumdar
 */
public class IntegerType implements TypeDescriptor {

    public final int getTypeCode() {
        return TYPE_INTEGER;
    }

    public final int getMaxLength() {
        return -1;
    }

	public int getScale() {
		return -1;
	}

	public DateFormat getDateFormat() {
		return null;
	}

	public TimeZone getTimeZone() {
		return null;
	}

	public int getStoredLength() {
		return 0;
	}

	public void retrieve(ByteBuffer bb) {
	}

	public void store(ByteBuffer bb) {
	}
	
	public int hashCode() {
		return TYPE_INTEGER;
	}
	
	public boolean equals(Object other) {
		if (other == this) 
			return true;
		if (other == null) 
			return false;
		if (other.getClass() == getClass()) 
			return true;
		return false;
	}

	public StringBuilder appendTo(StringBuilder sb) {
		return sb.append("IntegerType()");
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		return appendTo(sb).toString();
	}
	
}
