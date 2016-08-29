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
package org.simpledbm.common.util;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.simpledbm.common.api.registry.Storable;

/**
 * A format for String objects that is easier to persist.
 * 
 * @author Dibyendu Majumdar
 * @since 6 Mar 2010
 */
public final class StorableString implements Storable,
        Comparable<StorableString> {

    private final char[] data;

    public StorableString() {
        data = new char[0];
    }

    public StorableString(String s) {
        data = s.toCharArray();
    }

    public StorableString(char[] charArray) {
        this.data = charArray.clone();
    }

    public StorableString(StorableString s) {
        this.data = s.data;
    }

    public StorableString(ByteBuffer bb) {
        short n = bb.getShort();
        if (n > 0) {
            data = new char[n];
            bb.asCharBuffer().get(data);
            bb.position(bb.position() + n * TypeSize.CHARACTER);
        } else {
            data = new char[0];
        }
    }

    @Override
    public String toString() {
        return new String(data);
    }

    public int getStoredLength() {
        return data.length * TypeSize.CHARACTER + TypeSize.SHORT;
    }

    public void store(ByteBuffer bb) {
        short n = 0;
        if (data != null) {
            n = (short) data.length;
        }
        bb.putShort(n);
        if (n > 0) {
            bb.asCharBuffer().put(data);
            bb.position(bb.position() + n * TypeSize.CHARACTER);
        }
    }

    public int compareTo(StorableString o) {
        int len = (data.length <= o.data.length) ? data.length : o.data.length;
        for (int i = 0; i < len; i++) {
            int result = data[i] - o.data[i];
            if (result != 0) {
                return result;
            }
        }
        return data.length - o.data.length;
    }

    public int length() {
        return data.length;
    }

    public char get(int offset) {
        return data[offset];
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final StorableString other = (StorableString) obj;
        return compareTo(other) == 0;
    }
}
