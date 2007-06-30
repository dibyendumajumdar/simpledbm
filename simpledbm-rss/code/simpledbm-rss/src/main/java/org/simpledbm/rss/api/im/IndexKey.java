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
package org.simpledbm.rss.api.im;

import org.simpledbm.rss.api.st.Storable;

/**
 * Specifies the requirements to be met by Index Keys. Index Keys
 * must be Comparable and Storable. Note that this interface does not say
 * anything about the contents of the key, in particular it says
 * nothing about multi-attribute keys. This is deliberate; we want the
 * interface to be as generic as possible. 
 * 
 * @author Dibyendu Majumdar
 * @since Oct-2005
 */
public interface IndexKey extends Storable, Comparable<IndexKey> {

    /*
     * TODO: Rather than specifying the Key Comparison function as part of this
     * interface, a possibly better approach will be to specify it as part of
     * the IndexKeyFactory interface.
     */

    /**
     * Parses a string representation of the key and initializes 
     * its internal data; used primarily in test cases. The contents 
     * of the string is expected to match the toString() output from the
     * key.
     */
    void parseString(String string);
}
