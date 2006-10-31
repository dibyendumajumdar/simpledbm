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
 *    Project: www.simpledbm.org
 *    Author : Dibyendu Majumdar
 *    Email  : dibyendu@mazumdar.demon.co.uk
 */
package org.simpledbm.rss.util;

import java.nio.ByteBuffer;

import org.simpledbm.rss.api.st.Storable;

/**
 * A format for String objects that is easier to persist. 
 * 
 * @author Dibyendu Majumdar
 * @since 26-Jun-2005
 */
public final class ByteString implements Storable, Comparable<ByteString> {
    
    private byte[] bytes;
    
    public ByteString() {
    }

    public ByteString(String s) {
        bytes = s.getBytes();
    }
    
    public ByteString(byte[] bytes) {
        this.bytes = bytes.clone();
    }
    
    @Override
	public String toString() {
        if (bytes == null)
            return "";
        return new String(bytes);
    }
    
    public int getStoredLength() {
        return bytes.length + (Short.SIZE / Byte.SIZE);
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
    
    public void retrieve(ByteBuffer bb) {
        short n = bb.getShort();
        if (n > 0) {
            bytes = new byte[n];
            bb.get(bytes);
        }
        else {
            bytes = null;
        }
    }

	public int compareTo(ByteString o) {
		/*
		 * FIXME: This is inefficient 
		 */
		String s1 = new String(bytes);
		String s2 = new String(o.bytes);
		return s1.compareTo(s2);
	}
	
	public int length() {
		return bytes.length;
	}
	
	public byte get(int offset) {
		return bytes[offset];
	}
}
