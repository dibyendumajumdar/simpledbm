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
/*
 * Created on: Nov 15, 2005
 * Author: Dibyendu Majumdar
 */
package org.simpledbm.typesystem.impl;

import java.nio.ByteBuffer;
import java.util.HashMap;

import org.simpledbm.common.api.key.IndexKey;
import org.simpledbm.typesystem.api.DictionaryCache;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.RowFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.api.TypeFactory;

public class GenericRowFactory implements RowFactory {

    /**
     * Maintains a cache of row factories by container ID. When a row factory is
     * accessed for the first time for a particular container ID, it is put into
     * this hash table.
     */
    private HashMap<Integer, IndexRowFactory> rowCache = new HashMap<Integer, IndexRowFactory>();

    /**
     * The field factory instance that will be used to create fields.
     */
    private final TypeFactory fieldFactory;

    /**
     * A mapping between container Ids and TypeDescriptor[] is stored in the
     * Dictionary Cache
     */
    private final DictionaryCache dictionaryCache;

    public GenericRowFactory(TypeFactory fieldFactory,
            DictionaryCache dictionaryCache) {
        this.fieldFactory = fieldFactory;
        this.dictionaryCache = dictionaryCache;
    }

    public IndexKey newIndexKey(int containerId) {
        return newRow(containerId);
    }

    public IndexKey newIndexKey(int containerId, ByteBuffer bb) {
        return newRow(containerId, bb);
    }

    private synchronized IndexRowFactory getIndexRowFactory(int containerId) {
        IndexRowFactory rowFactory = rowCache.get(containerId);
        if (rowFactory == null) {
            TypeDescriptor[] rowTypeDesc = getTypeDescriptor(containerId);
            rowFactory = new IndexRowFactory(fieldFactory, rowTypeDesc);
            rowCache.put(containerId, rowFactory);
        }
        return rowFactory;
    }

    public synchronized Row newRow(int containerId) {
        IndexRowFactory rowFactory = getIndexRowFactory(containerId);
        return rowFactory.makeRow();
    }

    public synchronized Row newRow(int containerId, ByteBuffer bb) {
        IndexRowFactory rowFactory = getIndexRowFactory(containerId);
        return rowFactory.makeRow(bb);
    }

    public IndexKey maxIndexKey(int keytype) {
        GenericRow row = (GenericRow) newIndexKey(keytype);
        for (int i = 0; i < row.getNumberOfColumns(); i++) {
            row.setPositiveInfinity(i);
        }
        return row;
    }

    public IndexKey minIndexKey(int keytype) {
        GenericRow row = (GenericRow) newIndexKey(keytype);
        for (int i = 0; i < row.getNumberOfColumns(); i++) {
            row.setNegativeInfinity(i);
        }
        return row;
    }

    protected TypeDescriptor[] getTypeDescriptor(int keytype) {
        return dictionaryCache.getTypeDescriptor(keytype);
    }

    public void registerRowType(int keytype, TypeDescriptor[] rowTypeDesc) {
        dictionaryCache.registerRowType(keytype, rowTypeDesc);
    }

    public void unregisterRowType(int keytype) {
        dictionaryCache.unregisterRowType(keytype);
    }

    public DictionaryCache getDictionaryCache() {
        return dictionaryCache;
    }

    static class IndexRowFactory {

        final TypeDescriptor[] rowTypeDesc;

        final TypeFactory fieldFactory;

        public IndexRowFactory(TypeFactory fieldFactory,
                TypeDescriptor[] rowTypeDesc) {
            this.fieldFactory = fieldFactory;
            this.rowTypeDesc = rowTypeDesc;
        }

        public GenericRow makeRow() {
            return new GenericRow(fieldFactory, rowTypeDesc);
        }

        public GenericRow makeRow(ByteBuffer bb) {
            return new GenericRow(fieldFactory, rowTypeDesc, bb);
        }
    }

    public IndexKey parseIndexKey(int arg0, String arg1) {
        throw new UnsupportedOperationException();
    }
}
