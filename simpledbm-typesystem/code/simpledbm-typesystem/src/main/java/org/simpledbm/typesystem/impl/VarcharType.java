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
 *    Linking this library statically or dynamically with other modules 
 *    is making a combined work based on this library. Thus, the terms and
 *    conditions of the GNU General Public License cover the whole
 *    combination.
 *    
 *    As a special exception, the copyright holders of this library give 
 *    you permission to link this library with independent modules to 
 *    produce an executable, regardless of the license terms of these 
 *    independent modules, and to copy and distribute the resulting 
 *    executable under terms of your choice, provided that you also meet, 
 *    for each linked independent module, the terms and conditions of the 
 *    license of that module.  An independent module is a module which 
 *    is not derived from or based on this library.  If you modify this 
 *    library, you may extend this exception to your version of the 
 *    library, but you are not obligated to do so.  If you do not wish 
 *    to do so, delete this exception statement from your version.
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

import org.simpledbm.common.util.TypeSize;
import org.simpledbm.typesystem.api.TypeDescriptor;

/**
 * The VarChar type represents a variable length byte string, that has a
 * specified maximum length.
 * 
 * @author Dibyendu Majumdar
 */
public class VarcharType implements TypeDescriptor {

    int maxLength;

    public VarcharType() {
    }

    public VarcharType(int maxLength) {
        this.maxLength = maxLength;
    }

    public VarcharType(ByteBuffer bb) {
        maxLength = bb.getInt();
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

    public int getStoredLength() {
        return TypeSize.INTEGER;
    }

    //	public void retrieve(ByteBuffer bb) {
    //		maxLength = bb.getInt();
    //	}

    public void store(ByteBuffer bb) {
        bb.putInt(maxLength);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + maxLength;
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
        final VarcharType other = (VarcharType) obj;
        if (maxLength != other.maxLength)
            return false;
        return true;
    }

    public StringBuilder appendTo(StringBuilder sb) {
        return sb.append("VarcharType(maxLength=").append(maxLength)
                .append(")");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        return appendTo(sb).toString();
    }
}
