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

/**
 * @author Dibyendu
 * @since 20 Nov 2006
 */
public class BitwiseOperations {

    /**
     * Gets the specified bit (0-31) from the integer argument.
     * @param x Integer argument
     * @param p Position of the bit (0-31)
     */
    static final int getbit(final int x, final int p) {
        // TODO return value should be boolean, not the integer
        return (x & (1 << (31 - p))) == 0 ? 0 : 1;
    }

    /**
     * Sets the specified bit (0-31) in the integer argument.
     * @param x Integer argument
     * @param p Position of the bit to be set.
     * @return Modified integer
     */
    static final int setbit(final int x, final int p) {
        return x | (1 << (31 - p));
    }

    /**
     * Clears a range of bits in the specified integer.
     * @param x Integer argument
     * @param p Position (0-31)
     * @param n Number of bits to clear
     * @return Modified integer
     */
    static final int clearbits(final int x, final int p, final int n) {
        return x & ~((~0 << (32 - n)) >>> p);
    }
    
    public static void main(String args[]) {
    	
    	int value = 0;
    	
    	for (int j = 0; j < 32; j++) {
    		value = setbit(value, j);
    		for (int i = 0; i < 32; i++) {
    			System.err.print(getbit(value, i));
    		}
    		System.err.println();
    	}
    }
    
}
