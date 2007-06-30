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

/**
 * Specifies sizes of various types in bytes.
 */
public interface TypeSize {

    final int BYTE = 1;
    final int CHARACTER = Character.SIZE / Byte.SIZE;
    final int SHORT = Short.SIZE / Byte.SIZE;
    final int INTEGER = Integer.SIZE / Byte.SIZE;
    final int LONG = Long.SIZE / Byte.SIZE;
    final int FLOAT = Float.SIZE / Byte.SIZE;
    final int DOUBLE = Double.SIZE / Byte.SIZE;

}
