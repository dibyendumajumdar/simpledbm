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
 * Created on: Nov 14, 2005
 * Author: Dibyendu Majumdar
 */
package org.simpledbm.typesystem.impl;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Date;

import org.simpledbm.common.util.TypeSize;
import org.simpledbm.typesystem.api.DataValue;
import org.simpledbm.typesystem.api.TypeDescriptor;

abstract class BaseDataValue implements DataValue {

    private static final int NULL_FIELD = 1;
    private static final int MINUS_INFINITY_FIELD = 2;
    private static final int VALUE_FIELD = 4;
    private static final int PLUS_INFINITY_FIELD = 8;

    private static final String NULL_VALUE = "null";
    private static final String MAX_VALUE = "+infinity";
    private static final String MIN_VALUE = "-infinity";

    private byte statusByte = 0;
    private final TypeDescriptor typeDesc;

    protected BaseDataValue(BaseDataValue other) {
        this.statusByte = other.statusByte;
        this.typeDesc = other.typeDesc;
    }

    protected BaseDataValue(TypeDescriptor typeDesc) {
        statusByte = NULL_FIELD;
        this.typeDesc = typeDesc;
    }

    protected BaseDataValue(TypeDescriptor typeDesc, ByteBuffer bb) {
        this.typeDesc = typeDesc;
        statusByte = bb.get();
    }

    public int getInt() {
        throw new UnsupportedOperationException();
    }

    public void setInt(Integer integer) {
        throw new UnsupportedOperationException();
    }

    public String getString() {
        throw new UnsupportedOperationException();
    }

    public void setString(String string) {
        throw new UnsupportedOperationException();
    }

    public BigDecimal getBigDecimal() {
        throw new UnsupportedOperationException();
    }

    public BigInteger getBigInteger() {
        throw new UnsupportedOperationException();
    }

    public Date getDate() {
        throw new UnsupportedOperationException();
    }

    public long getLong() {
        throw new UnsupportedOperationException();
    }

    public void setBigDecimal(BigDecimal d) {
        throw new UnsupportedOperationException();
    }

    public void setBigInteger(BigInteger i) {
        throw new UnsupportedOperationException();
    }

    public void setDate(Date date) {
        throw new UnsupportedOperationException();
    }

    public void setLong(long l) {
        throw new UnsupportedOperationException();
    }

    public byte[] getBytes() {
        throw new UnsupportedOperationException();
    }

    public void setBytes(byte[] bytes) {
        throw new UnsupportedOperationException();
    }

    //	public void retrieve(ByteBuffer bb) {
    //        statusByte = bb.get();
    //    }

    public void store(ByteBuffer bb) {
        bb.put(statusByte);
    }

    public int getStoredLength() {
        return TypeSize.BYTE;
    }

    protected int compare(BaseDataValue o) {
        return statusByte - o.statusByte;
    }

    public int compareTo(DataValue other) {
        if (this == other) {
            return 0;
        }
        if (other == null) {
            throw new IllegalArgumentException();
        }
        if (!(other instanceof BaseDataValue)) {
            throw new ClassCastException("Cannot cast " + other.getClass()
                    + " to " + this.getClass());
        }
        return compare((BaseDataValue) other);
    }

    public final boolean isNull() {
        return (statusByte & NULL_FIELD) != 0;
    }

    public final void setNull() {
        statusByte = NULL_FIELD;
    }

    public final boolean isNegativeInfinity() {
        return (statusByte & MINUS_INFINITY_FIELD) != 0;
    }

    public final boolean isPositiveInfinity() {
        return (statusByte & PLUS_INFINITY_FIELD) != 0;
    }

    public final void setNegativeInfinity() {
        statusByte = MINUS_INFINITY_FIELD;
    }

    public final void setPositiveInfinity() {
        statusByte = PLUS_INFINITY_FIELD;
    }

    public final boolean isValue() {
        return (statusByte & VALUE_FIELD) != 0;
    }

    protected final void setValue() {
        statusByte = VALUE_FIELD;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null) {
            throw new IllegalArgumentException();
        }
        if (!(o instanceof BaseDataValue)) {
            throw new ClassCastException("Cannot cast " + o.getClass() + " to "
                    + this.getClass());
        }
        return compare((BaseDataValue) o) == 0;
    }

    public final TypeDescriptor getType() {
        return typeDesc;
    }

    public String toString() {
        if (isNull()) {
            return NULL_VALUE;
        }
        if (isPositiveInfinity()) {
            return MAX_VALUE;
        }
        if (isNegativeInfinity()) {
            return MIN_VALUE;
        }
        return "";
    }

    public StringBuilder appendTo(StringBuilder sb) {
        if (isNull()) {
            return sb.append(NULL_VALUE);
        }
        if (isPositiveInfinity()) {
            return sb.append(MAX_VALUE);
        }
        if (isNegativeInfinity()) {
            return sb.append(MIN_VALUE);
        }
        return sb;
    }

}
