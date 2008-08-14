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
package org.simpledbm.typesystem.api;

import java.text.DateFormat;
import java.util.TimeZone;

import org.simpledbm.rss.api.st.Storable;
import org.simpledbm.rss.util.Dumpable;

/**
 * Provides type information.
 * @author Dibyendu Majumdar
 */
public interface TypeDescriptor extends Storable, Dumpable {

    static final int TYPE_INTEGER = 1;
    static final int TYPE_VARCHAR = 2;
    static final int TYPE_NUMBER = 3;
    static final int TYPE_LONG_INTEGER = 4;
    static final int TYPE_DATETIME = 5;
    static final int TYPE_BINARY = 6;

    /**
     * Returns a unique integer code for the type. This is useful for 
     * serializing the type.
     */
    int getTypeCode();
    
    /**
     * Returns the maximum length of any value of this type.
     */
    int getMaxLength();

    /**
     * Returns the scale for certain numeric types. Scale defines the 
     * number of digits to the right of the decimal point.
     */
    int getScale();
    
    /**
     * Returns the timezone associated with the type - only applicable for
     * date and time types.
     */
    TimeZone getTimeZone();

    /**
     * Returns the date formatter object associated with the type - only
     * applicable for date and time types. 
     */
	DateFormat getDateFormat();
	
}
