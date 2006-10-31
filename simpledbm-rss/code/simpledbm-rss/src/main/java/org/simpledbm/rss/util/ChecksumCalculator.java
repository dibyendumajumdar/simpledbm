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

import java.util.zip.Adler32;
import java.util.zip.Checksum;

/**
 * Utility for calculating checksums for a range of bytes.
 * 
 * @author Dibyendu Majumdar
 */
public final class ChecksumCalculator {
    
    public static long compute(byte[] data, int start, int length) {
        Checksum checksum = new Adler32();
        checksum.update(data, start, length);
        return checksum.getValue();
    }

}
