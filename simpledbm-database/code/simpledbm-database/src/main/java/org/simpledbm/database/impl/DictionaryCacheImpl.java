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
package org.simpledbm.database.impl;

import static org.simpledbm.database.impl.DatabaseImpl.m_ED0013;

import java.util.HashMap;

import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.exception.DatabaseException;
import org.simpledbm.typesystem.api.DictionaryCache;
import org.simpledbm.typesystem.api.TypeDescriptor;

public class DictionaryCacheImpl implements DictionaryCache {

    private final DatabaseImpl database;

    /**
     * Contains a mapping of container IDs to row type descriptors.
     */
    private HashMap<Integer, TypeDescriptor[]> typeDescMap = new HashMap<Integer, TypeDescriptor[]>();

    public DictionaryCacheImpl(DatabaseImpl database) {
        this.database = database;
    }

    private synchronized TypeDescriptor[] findTypeDescriptor(int keytype) {
        return typeDescMap.get(keytype);
    }

    public TypeDescriptor[] getTypeDescriptor(int containerId) {
        TypeDescriptor rowType[] = findTypeDescriptor(containerId);
        if (rowType == null) {
            /*
             * Normally, the table definitions are always accessed from the data
             * dictionary cache. However, during restart recovery it is possible
             * that the table definition will be needed prior to initialization
             * of the dictionary cache. To allow for this, we load the table
             * definition here.
             * Note that this will also populate the dictionary cache
             */
            database.retrieveTableDefinition(containerId);
        }
        /*
         * We may be looking for an index row type, or a table
         * row type. So let's look in the cache again.
         */
        rowType = findTypeDescriptor(containerId);
        if (rowType == null) {
            this.database.exceptionHandler.errorThrow(getClass().getName(),
                    "getTypeDescriptor", new DatabaseException(
                            new MessageInstance(m_ED0013, containerId)));
        }
        return rowType;
    }

    public synchronized void registerRowType(int keytype,
            TypeDescriptor[] rowTypeDesc) {
        typeDescMap.put(keytype, rowTypeDesc);
    }

    public synchronized void unregisterRowType(int keytype) {
        typeDescMap.remove(keytype);
    }

}
