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
            this.database.exceptionHandler.errorThrow(getClass(),
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
