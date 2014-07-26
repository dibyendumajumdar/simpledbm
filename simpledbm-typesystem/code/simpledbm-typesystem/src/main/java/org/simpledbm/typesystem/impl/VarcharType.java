/**
 * DO NOT REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Contributor(s):
 *
 * The Original Software is SimpleDBM (www.simpledbm.org).
 * The Initial Developer of the Original Software is Dibyendu Majumdar.
 *
 * Portions Copyright 2005-2014 Dibyendu Majumdar. All Rights Reserved.
 *
 * The contents of this file are subject to the terms of the
 * Apache License Version 2 (the "APL"). You may not use this
 * file except in compliance with the License. A copy of the
 * APL may be obtained from:
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the APL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the APL, the GPL or the LGPL.
 *
 * Copies of GPL and LGPL may be obtained from:
 * http://www.gnu.org/licenses/license-list.html
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
