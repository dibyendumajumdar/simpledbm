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
/*
 * Created on: Nov 15, 2005
 * Author: Dibyendu Majumdar
 */
package org.simpledbm.typesystem.impl;

import java.nio.ByteBuffer;
import java.util.HashMap;

import org.simpledbm.rss.api.im.IndexKey;
import org.simpledbm.typesystem.api.DictionaryCache;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.RowFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.api.TypeFactory;

public class GenericRowFactory implements RowFactory {

	/**
	 * Maintains a cache of row factories by container ID.
	 * When a row factory is accessed for the first time for a particular container ID, it 
	 * is put into this hash table. 
	 */
    private HashMap<Integer, IndexRowFactory> rowCache = new HashMap<Integer, IndexRowFactory>();
    
    /**
     * The field factory instance that will be used to create fields.
     */
    private final TypeFactory fieldFactory;
    
    /**
     * A mapping between container Ids and TypeDescriptor[] is
     * stored in the Dictionary Cache
     */
    private final DictionaryCache dictionaryCache;
    
    public GenericRowFactory(TypeFactory fieldFactory, DictionaryCache dictionaryCache) {
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

    public DictionaryCache getDictionaryCache() {
		return dictionaryCache;
	}

	static class IndexRowFactory {

        final TypeDescriptor[] rowTypeDesc;
        
        final TypeFactory fieldFactory;
        
        public IndexRowFactory(TypeFactory fieldFactory, TypeDescriptor[] rowTypeDesc) {
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
