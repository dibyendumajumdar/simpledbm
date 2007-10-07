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
 *    Email  : dibyendu@mazumdar.demon.co.uk
 */
/*
 * Created on: Nov 16, 2005
 * Author: Dibyendu Majumdar
 */
package org.simpledbm.typesystem.impl;

import java.text.DateFormat;
import java.util.TimeZone;

import org.simpledbm.typesystem.api.TypeDescriptor;

/**
 * The VarChar type represents a variable length byte string, that has a specified
 * maximum length.
 * @author Dibyendu Majumdar
 */
public class VarcharType implements TypeDescriptor {

    final int maxLength;
    
    public VarcharType(int maxLength) {
        this.maxLength = maxLength;
    }
    
    public final int getTypeCode() {
        return TYPE_VARCHAR;
    }

    public final int getMaxLength() {
        return maxLength;
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
}
