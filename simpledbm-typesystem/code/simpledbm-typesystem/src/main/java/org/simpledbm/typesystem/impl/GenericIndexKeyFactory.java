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
import org.simpledbm.rss.api.im.IndexKeyFactory;
import org.simpledbm.typesystem.api.Field;
import org.simpledbm.typesystem.api.FieldFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;

public class GenericIndexKeyFactory implements IndexKeyFactory {

    private HashMap<Integer, IndexRowFactory> rowCache = new HashMap<Integer, IndexRowFactory>();
    
    private final FieldFactory fieldFactory;
    
    private HashMap<Integer, TypeDescriptor[]> typeDescMap = new HashMap<Integer, TypeDescriptor[]>();
    
    public GenericIndexKeyFactory(FieldFactory fieldFactory) {
        this.fieldFactory = fieldFactory;
    }
    
    public IndexKey newIndexKey() {
        return null;
    }

    public IndexKey maxIndexKey() {
        return null;
    }
    
    public IndexKey newIndexKey(int keytype) {
        IndexRowFactory rowFactory = rowCache.get(keytype);
        if (rowFactory == null) {
            TypeDescriptor[] rowTypeDesc = getTypeDescriptor(keytype);
            rowFactory = new IndexRowFactory(fieldFactory, rowTypeDesc);
            rowCache.put(keytype, rowFactory);
        }
        return rowFactory.makeRow();
    }

    public IndexKey maxIndexKey(int keytype) {
    	IndexRow row = (IndexRow) newIndexKey(keytype);
    	for (int i = 0; i < row.getNumberOfFields(); i++) {
    		Field field = row.get(i);
    		field.setPositiveInfinity();
    	}
    	return row;
    }
    
    TypeDescriptor[] getTypeDescriptor(int keytype) {
        return typeDescMap.get(keytype);
    }
    
    public void registerRowType(int keytype, TypeDescriptor[] rowTypeDesc) {
    	typeDescMap.put(keytype, rowTypeDesc);
    }

    static class IndexRowFactory {

        final TypeDescriptor[] rowTypeDesc;
        
        final FieldFactory fieldFactory;
        
        public IndexRowFactory(FieldFactory fieldFactory, TypeDescriptor[] rowTypeDesc) {
            this.fieldFactory = fieldFactory;
            this.rowTypeDesc = rowTypeDesc;
        }
        
        public IndexRow makeRow() {
            return new IndexRow(fieldFactory, rowTypeDesc);
        }

    }
    
}
