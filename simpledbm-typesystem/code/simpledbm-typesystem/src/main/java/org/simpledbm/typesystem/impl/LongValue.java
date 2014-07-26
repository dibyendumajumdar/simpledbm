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

import org.simpledbm.common.util.TypeSize;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.typesystem.api.DataValue;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.api.TypeException;

public class LongValue extends BaseDataValue {

    long i;

    LongValue(LongValue other) {
        super(other);
        this.i = other.i;
    }

    public LongValue(TypeDescriptor typeDesc) {
        super(typeDesc);
    }

    public LongValue(TypeDescriptor typeDesc, ByteBuffer bb) {
        super(typeDesc, bb);
        if (isValue()) {
            i = bb.getLong();
        }
    }

    @Override
    public String toString() {
        return getString();
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
    public void store(ByteBuffer bb) {
        super.store(bb);
        if (isValue()) {
            bb.putLong(i);
        }
    }

    @Override
    public int getInt() {
        if (!isValue()) {
            return 0;
        }
        if (i > Integer.MAX_VALUE || i < Integer.MIN_VALUE) {
            throw new TypeException(new MessageInstance(
                    TypeSystemFactoryImpl.outOfRange, i, Integer.MIN_VALUE,
                    Integer.MAX_VALUE));
        }
        return (int) i;
    }

    @Override
    public String getString() {
        if (isValue()) {
            return Long.toString(i);
        }
        return super.toString();
    }

    @Override
    public void setInt(Integer integer) {
        i = integer;
        setValue();
    }

    @Override
    public long getLong() {
        if (!isValue()) {
            return 0;
        }
        return i;
    }

    @Override
    public void setLong(long l) {
        i = l;
        setValue();
    }

    @Override
    public void setString(String string) {
        setInt(Integer.parseInt(string));
    }

    protected int compare(LongValue o) {
        int comp = super.compare(o);
        if (comp != 0 || !isValue()) {
            return comp;
        }
        if (i > o.i)
            return 1;
        else if (i == o.i)
            return 0;
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
        if (!(o instanceof LongValue)) {
            throw new ClassCastException("Cannot cast " + o.getClass() + " to "
                    + this.getClass());
        }
        return compare((LongValue) o);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null) {
            throw new IllegalArgumentException();
        }
        if (!(o instanceof LongValue)) {
            throw new ClassCastException("Cannot cast " + o.getClass() + " to "
                    + this.getClass());
        }
        return compare((LongValue) o) == 0;
    }

    public DataValue cloneMe() {
        return new LongValue(this);
    }

    @Override
    public StringBuilder appendTo(StringBuilder sb) {
        if (isValue()) {
            return sb.append(i);
        }
        return super.appendTo(sb);
    }

}
