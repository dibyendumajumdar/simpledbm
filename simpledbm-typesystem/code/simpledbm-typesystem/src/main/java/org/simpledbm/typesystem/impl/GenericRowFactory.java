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
/*
 * Created on: Nov 15, 2005
 * Author: Dibyendu Majumdar
 */
package org.simpledbm.typesystem.impl;

import java.util.HashMap;

import org.simpledbm.rss.api.im.IndexKey;
import org.simpledbm.typesystem.api.Field;
import org.simpledbm.typesystem.api.FieldFactory;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.RowFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;

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
    private final FieldFactory fieldFactory;
    
    /**
     * Contains a mapping of container IDs to row type descriptors.
     */
    private HashMap<Integer, TypeDescriptor[]> typeDescMap = new HashMap<Integer, TypeDescriptor[]>();
    
    public GenericRowFactory(FieldFactory fieldFactory) {
        this.fieldFactory = fieldFactory;
    }
    
    public IndexKey newIndexKey(int containerId) {
    	return newRow(containerId);
    }

    public synchronized Row newRow(int containerId) {
        IndexRowFactory rowFactory = rowCache.get(containerId);
        if (rowFactory == null) {
            TypeDescriptor[] rowTypeDesc = getTypeDescriptor(containerId);
            rowFactory = new IndexRowFactory(fieldFactory, rowTypeDesc);
            rowCache.put(containerId, rowFactory);
        }
        return rowFactory.makeRow();
    }
    
    public IndexKey maxIndexKey(int keytype) {
    	GenericRow row = (GenericRow) newIndexKey(keytype);
    	for (int i = 0; i < row.getNumberOfFields(); i++) {
    		Field field = row.get(i);
    		field.setPositiveInfinity();
    	}
    	return row;
    }

    public IndexKey minIndexKey(int keytype) {
    	GenericRow row = (GenericRow) newIndexKey(keytype);
    	for (int i = 0; i < row.getNumberOfFields(); i++) {
    		Field field = row.get(i);
    		field.setNegativeInfinity();
    	}
    	return row;
    }
    
    protected synchronized TypeDescriptor[] getTypeDescriptor(int keytype) {
        return typeDescMap.get(keytype);
    }
    
    public synchronized void registerRowType(int keytype, TypeDescriptor[] rowTypeDesc) {
    	typeDescMap.put(keytype, rowTypeDesc);
    }

    static class IndexRowFactory {

        final TypeDescriptor[] rowTypeDesc;
        
        final FieldFactory fieldFactory;
        
        public IndexRowFactory(FieldFactory fieldFactory, TypeDescriptor[] rowTypeDesc) {
            this.fieldFactory = fieldFactory;
            this.rowTypeDesc = rowTypeDesc;
        }
        
        public GenericRow makeRow() {
            return new GenericRow(fieldFactory, rowTypeDesc);
        }

    }
    
}
