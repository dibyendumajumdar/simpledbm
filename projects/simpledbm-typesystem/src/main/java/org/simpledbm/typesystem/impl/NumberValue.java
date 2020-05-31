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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;

import org.simpledbm.common.util.TypeSize;
import org.simpledbm.typesystem.api.DataValue;
import org.simpledbm.typesystem.api.TypeDescriptor;

public class NumberValue extends BaseDataValue {

    BigDecimal d;

    public NumberValue(TypeDescriptor typeDesc) {
        super(typeDesc);
    }

    public NumberValue(NumberValue other) {
        super(other);
        this.d = new BigDecimal(other.d.unscaledValue(), other.d.scale());
    }

    public NumberValue(TypeDescriptor typeDesc, ByteBuffer bb) {
        super(typeDesc, bb);
        if (isValue()) {
            int scale = bb.get();
            int len = bb.get();
            byte[] data = new byte[len];
            bb.get(data);
            d = new BigDecimal(new BigInteger(data), scale);
        }
    }

    public DataValue cloneMe() {
        return new NumberValue(this);
    }

    protected int compare(NumberValue o) {
        int comp = super.compareTo(o);
        if (comp != 0 || !isValue()) {
            return comp;
        }
        return d.compareTo(o.d);
    }

    @Override
    public int compareTo(DataValue o) {
        if (o == this) {
            return 0;
        }
        if (o == null) {
            throw new IllegalArgumentException();
        }
        if (!(o instanceof NumberValue)) {
            throw new ClassCastException();
        }
        return compare((NumberValue) o);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o == null) {
            throw new IllegalArgumentException();
        }
        if (!(o instanceof NumberValue)) {
            throw new ClassCastException();
        }
        return compare((NumberValue) o) == 0;
    }

    @Override
    public int getInt() {
        if (isValue()) {
            return d.intValue();
        }
        return 0;
    }

    @Override
    public int getStoredLength() {
        int n = super.getStoredLength();
        if (isValue()) {
            BigInteger i = d.unscaledValue();
            /* 
             * Following length calculation is based
             * upon logic in BigInteger class (openjdk).
             */
            int byteLen = i.bitLength() / 8 + 1;
            //byte[] data = i.toByteArray();
            //int byteLen = data.length;
            n += TypeSize.BYTE * 2 + byteLen;
        }
        return n;
    }

    @Override
    public String getString() {
        if (!isValue()) {
            return super.toString();
        }
        return d.toString();
    }

    //	@Override
    //	public void retrieve(ByteBuffer bb) {
    //		super.retrieve(bb);
    //		if (isValue()) {
    //			int scale = bb.get();
    //			int len = bb.get();
    //			byte[] data = new byte[len];
    //			bb.get(data);
    //			d = new BigDecimal(new BigInteger(data), scale);
    //		}
    //	}

    @Override
    public void setInt(Integer integer) {
        d = new BigDecimal(integer);
        d = d.setScale(getType().getScale(), RoundingMode.HALF_UP);
        setValue();
    }

    @Override
    public void setString(String string) {
        d = new BigDecimal(string);
        d = d.setScale(getType().getScale(), RoundingMode.HALF_UP);
        setValue();
    }

    @Override
    public void store(ByteBuffer bb) {
        super.store(bb);
        if (isValue()) {
            byte[] data = d.unscaledValue().toByteArray();
            bb.put((byte) getType().getScale());
            bb.put((byte) data.length);
            bb.put(data);
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((!isValue()) ? 0 : d.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return getString();
    }

    @Override
    public StringBuilder appendTo(StringBuilder sb) {
        if (isValue()) {
            return sb.append(getString());
        }
        return super.appendTo(sb);
    }
}
