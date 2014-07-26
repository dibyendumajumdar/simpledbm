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

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.simpledbm.common.api.exception.SimpleDBMException;
import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.util.mcat.Message;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.common.util.mcat.MessageType;

/**
 * A format for String objects that is easier to persist. The string contents
 * are stored in UTF-8 format.
 * 
 * @author Dibyendu Majumdar
 * @since 26-Jun-2005
 */
public final class ByteString implements Storable, Comparable<ByteString> {

    final static Message illegalEncodingMessage = new Message(
            'C',
            'U',
            MessageType.ERROR,
            1,
            "Unexpected error: invalid encoding encountered when converting bytes to String");

    private final byte[] bytes;

    /**
     * Cached string representation
     */
    private volatile String s;

    public ByteString() {
        bytes = new byte[0];
    }

    public ByteString(String s) {
        try {
            this.s = s;
            bytes = s.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new SimpleDBMException(new MessageInstance(
                    illegalEncodingMessage), e);
        }
    }

    public ByteString(byte[] bytes) {
        // FIXME unsafe as incoming bytes may not be valid string
        this.bytes = bytes.clone();
    }

    public ByteString(ByteString s) {
        this.s = s.s;
        this.bytes = s.bytes.clone();
    }

    public ByteString(ByteBuffer bb) {
        short n = bb.getShort();
        if (n > 0) {
            bytes = new byte[n];
            bb.get(bytes);
        } else {
            bytes = new byte[0];
        }
    }

    @Override
    public String toString() {
        if (s != null) {
            return s;
        }
        try {
            synchronized (this) {
                if (null == s) {
                    s = new String(bytes, "UTF-8");
                }
            }
        } catch (UnsupportedEncodingException e) {
            throw new SimpleDBMException(new MessageInstance(
                    illegalEncodingMessage), e);
        }
        return s;
    }

    public final int getStoredLength() {
        return bytes.length + TypeSize.SHORT;
    }

    public final void store(ByteBuffer bb) {
        short n = 0;
        n = (short) bytes.length;
        bb.putShort(n);
        if (n > 0) {
            bb.put(bytes, 0, n);
        }
    }

    public final int compareTo(ByteString o) {
        int len = (bytes.length <= o.bytes.length) ? bytes.length
                : o.bytes.length;
        for (int i = 0; i < len; i++) {
            int result = bytes[i] - o.bytes[i];
            if (result != 0) {
                return result;
            }
        }
        return bytes.length - o.bytes.length;
    }

    public final int length() {
        return bytes.length;
    }

    public final byte get(int offset) {
        return bytes[offset];
    }

    @Override
    public final int hashCode() {
        final int PRIME = 31;
        int result = 1;
        result = PRIME * result + Arrays.hashCode(bytes);
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
        final ByteString other = (ByteString) obj;
        return compareTo(other) == 0;
    }
}
