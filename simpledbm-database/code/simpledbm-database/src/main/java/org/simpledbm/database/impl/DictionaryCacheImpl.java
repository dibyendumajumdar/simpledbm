package org.simpledbm.database.impl;

import java.util.HashMap;

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
    
    public TypeDescriptor[] getTypeDescriptor(int keytype) {
        TypeDescriptor rowType[] = findTypeDescriptor(keytype);
        if (rowType == null) {
        	/*
			 * Normally, the table definitions are always accessed from the data
			 * dictionary cache. However, during restart recovery it is possible
			 * that the table definition will be needed prior to initialization
			 * of the dictionary cache. To allow for this, we load the table
			 * definition here.
			 * Note that this will also populate the dictionary cache
			 */
            database.retrieveTableDefinition(keytype);
        }
        /*
         * We may be looking for an index row type, or a table
         * row type. So let's look in the cache again.
         */
        rowType = findTypeDescriptor(keytype);
        if (rowType == null) {
        	// FIXME Proper error message is needed (see ED0013)
        	throw new DatabaseException();
        }
        return rowType;
    }
    
    public synchronized void registerRowType(int keytype, TypeDescriptor[] rowTypeDesc) {
    	typeDescMap.put(keytype, rowTypeDesc);
    }
    
}
