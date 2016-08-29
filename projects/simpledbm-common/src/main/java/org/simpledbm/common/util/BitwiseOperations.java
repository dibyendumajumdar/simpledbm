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

/**
 * @author Dibyendu
 * @since 20 Nov 2006
 */
public class BitwiseOperations {

    /**
     * Gets the specified bit (0-31) from the integer argument.
     * 
     * @param x Integer argument
     * @param p Position of the bit (0-31)
     */
    static final int getbit(final int x, final int p) {
        // TODO return value should be boolean, not the integer
        return (x & (1 << (31 - p))) == 0 ? 0 : 1;
    }

    /**
     * Sets the specified bit (0-31) in the integer argument.
     * 
     * @param x Integer argument
     * @param p Position of the bit to be set.
     * @return Modified integer
     */
    static final int setbit(final int x, final int p) {
        return x | (1 << (31 - p));
    }

    /**
     * Clears a range of bits in the specified integer.
     * 
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
