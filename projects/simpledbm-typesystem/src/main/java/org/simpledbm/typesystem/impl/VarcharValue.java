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
import java.nio.ByteBuffer;

import org.simpledbm.common.util.TypeSize;
import org.simpledbm.typesystem.api.DataValue;
import org.simpledbm.typesystem.api.TypeDescriptor;

public class VarcharValue extends BaseDataValue {

    private char[] charArray;

    VarcharValue(VarcharValue other) {
        super(other);
        if (isValue()) {
            this.charArray = other.charArray.clone();
        } else {
            this.charArray = null;
        }
    }

    public VarcharValue(TypeDescriptor typeDesc) {
        super(typeDesc);
    }

    public VarcharValue(TypeDescriptor typeDesc, ByteBuffer bb) {
        super(typeDesc, bb);
        if (isValue()) {
            short n = bb.getShort();
            charArray = new char[n];
            for (int i = 0; i < charArray.length; i++) {
                charArray[i] = bb.getChar();
            }
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
            n += TypeSize.SHORT;
            n += charArray.length * TypeSize.CHARACTER;
        }
        return n;
    }

    @Override
    public void store(ByteBuffer bb) {
        super.store(bb);
        short n = 0;
        if (isValue()) {
            n = (short) charArray.length;
            bb.putShort(n);
            for (char c : charArray) {
                bb.putChar(c);
            }
        }
    }

    //    @Override
    //    public void retrieve(ByteBuffer bb) {
    //    	super.retrieve(bb);
    //    	if (isValue()) {
    //			short n = bb.getShort();
    //			charArray = new char[n];
    //			for (int i = 0; i < charArray.length; i++) {
    //				charArray[i] = bb.getChar();
    //			}
    //		}
    //    }

    @Override
    public int getInt() {
        if (!isValue()) {
            return 0;
        }
        return Integer.parseInt(toString());
    }

    @Override
    public String getString() {
        if (isValue()) {
            return new String(charArray);
        }
        return super.toString();
    }

    @Override
    public void setInt(Integer integer) {
        setString(integer.toString());
    }

    @Override
    public BigDecimal getBigDecimal() {
        if (!isValue()) {
            return null;
        }
        BigDecimal d = new BigDecimal(charArray);
        return d;
    }

    @Override
    public BigInteger getBigInteger() {
        if (!isValue()) {
            return null;
        }
        BigInteger i = new BigInteger(toString());
        return i;
    }

    @Override
    public long getLong() {
        if (!isValue()) {
            return 0;
        }
        return Long.valueOf(toString());
    }

    @Override
    public void setBigDecimal(BigDecimal d) {
        setString(d.toString());
    }

    @Override
    public void setBigInteger(BigInteger i) {
        setString(i.toString());
    }

    @Override
    public void setLong(long l) {
        setString(Long.toString(l));
    }

    @Override
    public void setString(String string) {
        if (string.length() > getType().getMaxLength()) {
            string = string.substring(0, getType().getMaxLength());
        }
        charArray = string.toCharArray();
        setValue();
    }

    protected int compare(VarcharValue other) {
        int comp = super.compare(other);
        if (comp != 0 || !isValue()) {
            return comp;
        }
        VarcharValue o = (VarcharValue) other;
        int len1 = charArray == null ? 0 : charArray.length;
        int len2 = o.charArray == null ? 0 : o.charArray.length;
        int prefixLen = Math.min(len1, len2);
        for (int i = 0; i < prefixLen; i++) {
            int c1 = charArray[i];
            int c2 = o.charArray[i];
            if (c1 != c2) {
                return c1 - c2;
            }
        }
        return len1 - len2;
    }

    @Override
    public int compareTo(DataValue other) {
        if (other == this) {
            return 0;
        }
        if (other == null) {
            throw new IllegalArgumentException();
        }
        if (!(other instanceof VarcharValue)) {
            throw new ClassCastException();
        }
        return compare((VarcharValue) other);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other == null) {
            throw new IllegalArgumentException();
        }
        if (!(other instanceof VarcharValue)) {
            throw new ClassCastException();
        }
        return compare((VarcharValue) other) == 0;
    }

    public int length() {
        if (isValue()) {
            return charArray.length;
        }
        return 0;
    }

    public DataValue cloneMe() {
        return new VarcharValue(this);
    }

    @Override
    public StringBuilder appendTo(StringBuilder sb) {
        if (isValue()) {
            return sb.append(getString());
        }
        return super.appendTo(sb);
    }
}
