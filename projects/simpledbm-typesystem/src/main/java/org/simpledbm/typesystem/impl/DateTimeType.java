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
package org.simpledbm.typesystem.impl;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.TimeZone;

import org.simpledbm.common.util.ByteString;
import org.simpledbm.typesystem.api.TypeDescriptor;

public class DateTimeType implements TypeDescriptor {

    final TimeZone timeZone;
    final SimpleDateFormat dateFormat;

    public DateTimeType(String timeZone, String format) {
        this.timeZone = TimeZone.getTimeZone(timeZone);
        this.dateFormat = new SimpleDateFormat(format, Locale.ENGLISH);
        this.dateFormat.setTimeZone(this.timeZone);
    }

    public DateTimeType(ByteBuffer bb) {
        ByteString bs = new ByteString(bb);
        timeZone = TimeZone.getTimeZone(bs.toString());
        bs = new ByteString(bb);
        dateFormat = new SimpleDateFormat(bs.toString(), Locale.ENGLISH);
        dateFormat.setTimeZone(timeZone);
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
        sb.append("DateTimeType(timeZone=").append(timeZone.getID()).append(
                ", dateFormat='").append(dateFormat.toPattern()).append("')");
        return sb;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        return appendTo(sb).toString();
    }
}
