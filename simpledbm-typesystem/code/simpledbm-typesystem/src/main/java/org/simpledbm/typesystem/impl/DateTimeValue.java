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
import java.text.ParseException;
import java.util.Date;

import org.simpledbm.common.util.TypeSize;
import org.simpledbm.typesystem.api.DataValue;
import org.simpledbm.typesystem.api.TypeDescriptor;

public class DateTimeValue extends BaseDataValue {

    long time;

    DateTimeValue(DateTimeValue other) {
        super(other);
        this.time = other.time;
    }

    DateTimeValue(TypeDescriptor typeDesc) {
        super(typeDesc);
    }

    DateTimeValue(TypeDescriptor typeDesc, ByteBuffer bb) {
        super(typeDesc, bb);
        if (isValue()) {
            time = bb.getLong();
        }
    }

    public DataValue cloneMe() {
        DateTimeValue clone = new DateTimeValue(this);
        return clone;
    }

    protected int compare(DateTimeValue o) {
        int comp = super.compare(o);
        if (comp != 0 || !isValue()) {
            return comp;
        }
        if (time == o.time)
            return 0;
        else if (time > o.time)
            return 1;
        else
            return -1;
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
            throw new ClassCastException("Cannot cast " + o.getClass() + " to "
                    + this.getClass());
        }
        return compare((DateTimeValue) o);
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
            throw new ClassCastException("Cannot cast " + o.getClass() + " to "
                    + this.getClass());
        }
        return compare((DateTimeValue) o) == 0;
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

    //	@Override
    //	public void retrieve(ByteBuffer bb) {
    //		super.retrieve(bb);
    //		if (isValue()) {
    //			time = bb.getLong();
    //		}
    //	}

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
        StringBuilder sb = new StringBuilder();
        return appendTo(sb).toString();
    }

    private DateTimeType getMyType() {
        return (DateTimeType) getType();
    }

    @Override
    public StringBuilder appendTo(StringBuilder sb) {
        if (isValue()) {
            return sb.append(getString());
        }
        return super.appendTo(sb);
    }
}
