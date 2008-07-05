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
 *    Email  : d dot majumdar at gmail dot com ignore
 */
package org.simpledbm.typesystem.impl;

import java.util.HashMap;

import org.simpledbm.typesystem.api.DictionaryCache;
import org.simpledbm.typesystem.api.TypeDescriptor;

public class SimpleDictionaryCache implements DictionaryCache {

    /**
     * Contains a mapping of container IDs to row type descriptors.
     */
    private HashMap<Integer, TypeDescriptor[]> typeDescMap = new HashMap<Integer, TypeDescriptor[]>();
    
    public synchronized TypeDescriptor[] getTypeDescriptor(int keytype) {
        return typeDescMap.get(keytype);
    }
    
    public synchronized void registerRowType(int keytype, TypeDescriptor[] rowTypeDesc) {
    	typeDescMap.put(keytype, rowTypeDesc);
    }
	
}
