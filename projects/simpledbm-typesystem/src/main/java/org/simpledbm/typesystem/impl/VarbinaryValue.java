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

public class VarbinaryValue extends BaseDataValue {

    private byte[] byteArray;

    VarbinaryValue(VarbinaryValue other) {
        super(other);
        if (isValue()) {
            this.byteArray = other.byteArray.clone();
        } else {
            this.byteArray = null;
        }
    }

    public VarbinaryValue(TypeDescriptor typeDesc) {
        super(typeDesc);
    }

    public VarbinaryValue(TypeDescriptor typeDesc, ByteBuffer bb) {
        super(typeDesc, bb);
        if (isValue()) {
            short n = bb.getShort();
            if (n < 0 || n > getType().getMaxLength()) {
                throw new TypeException(new MessageInstance(
                        TypeSystemFactoryImpl.outOfRange, n, 0, getType()
                                .getMaxLength()));
            }
            byteArray = new byte[n];
            bb.get(byteArray);
        }
    }

    @Override
    public String toString() {
        return getString();
    }

    @Override
    public byte[] getBytes() {
        if (!isValue()) {
            return null;
        }
        return byteArray.clone();
    }

    @Override
    public void setBytes(byte[] bytes) {
        if (bytes.length > getType().getMaxLength()) {
            byteArray = new byte[getType().getMaxLength()];
            System.arraycopy(bytes, 0, byteArray, 0, byteArray.length);
        } else {
            byteArray = bytes;
        }
        setValue();
    }

    @Override
    public int getStoredLength() {
        int n = super.getStoredLength();
        if (isValue()) {
            n += TypeSize.SHORT;
            n += byteArray.length * TypeSize.BYTE;
        }
        return n;
    }

    @Override
    public void store(ByteBuffer bb) {
        super.store(bb);
        short n = 0;
        if (isValue()) {
            n = (short) byteArray.length;
            bb.putShort(n);
            bb.put(byteArray);
        }
    }

    //	@Override
    //	public void retrieve(ByteBuffer bb) {
    //		super.retrieve(bb);
    //		if (isValue()) {
    //			short n = bb.getShort();
    //			if (n < 0 || n > getType().getMaxLength()) {
    //				throw new TypeException();
    //			}
    //			byteArray = new byte[n];
    //			bb.get(byteArray);
    //		}
    //	}

    /*
     *   @Interruptible
    665   public static String longAsHexString(long number) {
    666     char[] buf = new char[18];
    667     int index = 18;
    668     while (--index > 1) {
    669       int digit = (int) (number & 0x000000000000000fL);
    670       buf[index] = digit <= 9 ? (char) ('0' + digit) : (char) ('a' + digit - 10);
    671       number >>= 4;
    672     }
    673     buf[index--] = 'x';
    674     buf[index] = '0';
    675     return new String(buf);
    676   }
    677 
    678   **
    679    * Format a 32/64 bit number as "0x" followed by 8/16 hex digits.
    680    * Do this without referencing Integer or Character classes,
    681    * in order to avoid dynamic linking.
    682    * TODO: move this method to Services.
    683    * @param addr  The 32/64 bit number to format.
    684    * @return a String with the hex representation of an Address
    685    *
    686   @Interruptible
    687   public static String addressAsHexString(Address addr) {
    688     int len = 2 + (BITS_IN_ADDRESS >> 2);
    689     char[] buf = new char[len];
    690     while (--len > 1) {
    691       int digit = addr.toInt() & 0x0F;
    692       buf[len] = digit <= 9 ? (char) ('0' + digit) : (char) ('a' + digit - 10);
    693       addr = addr.toWord().rshl(4).toAddress();
    694     }
    695     buf[len--] = 'x';
    696     buf[len] = '0';
    697     return new String(buf);
    698   }

     */

    static String byteArrayToHexString(byte in[]) {
        /*
         * Following code is a modified version of:
         * http://www.devx.com/tips/Tip/13540
         */
        if (in.length == 0) {
            return "";
        }

        char pseudo[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
                'A', 'B', 'C', 'D', 'E', 'F' };
        StringBuilder out = new StringBuilder(in.length * 2);
        byte ch = 0x00;
        int i = 0;
        while (i < in.length) {
            ch = (byte) (in[i] & 0xF0); // Strip off high nibble
            ch = (byte) (ch >>> 4); // shift the bits down
            ch = (byte) (ch & 0x0F); // must do this is high order bit is on!
            out.append(pseudo[(int) ch]); // convert the nibble to a Character
            ch = (byte) (in[i] & 0x0F); // Strip off low nibble
            out.append(pseudo[(int) ch]); // convert the nibble to a Character
            i++;
        }
        return out.toString();
    }

    static byte getValue(char c) {
        switch (c) {
        case '0':
            return 0;
        case '1':
            return 1;
        case '2':
            return 2;
        case '3':
            return 3;
        case '4':
            return 4;
        case '5':
            return 5;
        case '6':
            return 6;
        case '7':
            return 7;
        case '8':
            return 8;
        case '9':
            return 9;
        case 'A':
        case 'a':
            return 10;
        case 'B':
        case 'b':
            return 11;
        case 'C':
        case 'c':
            return 12;
        case 'D':
        case 'd':
            return 13;
        case 'E':
        case 'e':
            return 14;
        case 'F':
        case 'f':
            return 15;
        }
        throw new IllegalArgumentException();
    }

    @Override
    public String getString() {
        if (isValue()) {
            return byteArrayToHexString(byteArray);
        }
        return super.toString();
    }

    static byte[] hexStringToByteArray(String s) {

        byte[] bytes = new byte[s.length() / 2];
        for (int i = 0, j = 0; i < s.length(); i += 2, j++) {
            byte b = (byte) ((getValue(s.charAt(i)) << 4) & 0xF0);
            b |= (byte) (getValue(s.charAt(i + 1)) & 0x0F);
            bytes[j] = b;
        }
        return bytes;
    }

    @Override
    public void setString(String string) {
        byte[] bytes = hexStringToByteArray(string);
        setBytes(bytes);
    }

    protected int compare(VarbinaryValue other) {
        int comp = super.compare(other);
        if (comp != 0 || !isValue()) {
            return comp;
        }
        VarbinaryValue o = (VarbinaryValue) other;
        int n1 = byteArray == null ? 0 : byteArray.length;
        int n2 = o.byteArray == null ? 0 : o.byteArray.length;
        int prefixLen = Math.min(n1, n2);
        for (int i = 0; i < prefixLen; i++) {
            int rc = byteArray[i] - o.byteArray[i];
            if (rc != 0) {
                return rc;
            }
        }
        return n1 - n2;
    }

    @Override
    public int compareTo(DataValue other) {
        if (other == this) {
            return 0;
        }
        if (other == null) {
            throw new IllegalArgumentException();
        }
        if (!(other instanceof VarbinaryValue)) {
            throw new ClassCastException();
        }
        return compare((VarbinaryValue) other);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other == null) {
            throw new IllegalArgumentException();
        }
        if (!(other instanceof VarbinaryValue)) {
            throw new ClassCastException();
        }
        return compare((VarbinaryValue) other) == 0;
    }

    public int length() {
        if (isValue()) {
            return byteArray.length;
        }
        return 0;
    }

    public DataValue cloneMe() {
        return new VarbinaryValue(this);
    }

    @Override
    public StringBuilder appendTo(StringBuilder sb) {
        if (isValue()) {
            return sb.append(getString());
        }
        return super.appendTo(sb);
    }
}
