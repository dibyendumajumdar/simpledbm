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

import org.simpledbm.typesystem.api.TypeDescriptor;

/**
 * The LongType provides storage for a 64-bit integer.
 * 
 * @author Dibyendu Majumdar
 */
public class LongType implements TypeDescriptor {

    public LongType() {
    }

    public LongType(ByteBuffer bb) {
    }

    public final int getTypeCode() {
        return TYPE_LONG_INTEGER;
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

    //	public void retrieve(ByteBuffer bb) {
    //	}

    public void store(ByteBuffer bb) {
    }

    public int hashCode() {
        return TYPE_LONG_INTEGER;
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
        return sb.append("LongType()");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        return appendTo(sb).toString();
    }

}
