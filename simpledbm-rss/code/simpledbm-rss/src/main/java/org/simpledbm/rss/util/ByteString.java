/***
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program; if not, write to the Free Software
 *    Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *    
 *    Linking this library statically or dynamically with other modules 
 *    is making a combined work based on this library. Thus, the terms and
 *    conditions of the GNU General Public License cover the whole
 *    combination.
 *    
 *    As a special exception, the copyright holders of this library give 
 *    you permission to link this library with independent modules to 
 *    produce an executable, regardless of the license terms of these 
 *    independent modules, and to copy and distribute the resulting 
 *    executable under terms of your choice, provided that you also meet, 
 *    for each linked independent module, the terms and conditions of the 
 *    license of that module.  An independent module is a module which 
 *    is not derived from or based on this library.  If you modify this 
 *    library, you may extend this exception to your version of the 
 *    library, but you are not obligated to do so.  If you do not wish 
 *    to do so, delete this exception statement from your version.
 *
 *    Project: www.simpledbm.org
 *    Author : Dibyendu Majumdar
 *    Email  : d dot majumdar at gmail dot com ignore
 */
package org.simpledbm.rss.util;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.simpledbm.rss.api.exception.RSSException;
import org.simpledbm.rss.api.registry.Storable;

/**
 * A format for String objects that is easier to persist. 
 * 
 * @author Dibyendu Majumdar
 * @since 26-Jun-2005
 */
public final class ByteString implements Storable, Comparable<ByteString> {

    private final byte[] bytes;

    public ByteString() {
        bytes = new byte[0];
    }

    public ByteString(String s) {
        try {
            bytes = s.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RSSException(e);
        }
    }

    public ByteString(byte[] bytes) {
        this.bytes = bytes.clone();
    }
    
    public ByteString(ByteString s) {
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
        try {
            return new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RSSException(e);
        }
    }

    public int getStoredLength() {
        return bytes.length + TypeSize.SHORT;
    }

    public void store(ByteBuffer bb) {
        short n = 0;
        if (bytes != null) {
            n = (short) bytes.length;
        }
        bb.putShort(n);
        if (n > 0) {
            bb.put(bytes, 0, n);
        }
    }

    public int compareTo(ByteString o) {
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

    public int length() {
        return bytes.length;
    }

    public byte get(int offset) {
        return bytes[offset];
    }

    @Override
    public int hashCode() {
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
        if (!Arrays.equals(bytes, other.bytes))
            return false;
        return true;
    }
}
