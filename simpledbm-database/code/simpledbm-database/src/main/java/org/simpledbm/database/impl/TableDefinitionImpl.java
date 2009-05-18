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

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.simpledbm.database.api.Database;
import org.simpledbm.database.api.IndexDefinition;
import org.simpledbm.database.api.TableDefinition;
import org.simpledbm.exception.DatabaseException;
import org.simpledbm.rss.api.platform.PlatformObjects;
import org.simpledbm.rss.api.registry.Storable;
import org.simpledbm.rss.util.ByteString;
import org.simpledbm.rss.util.TypeSize;
import org.simpledbm.rss.util.logging.Logger;
import org.simpledbm.rss.util.mcat.MessageCatalog;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.RowFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;

/**
 * Encapsulates a table definition and provides methods to work with table and
 * indexes associated with the table.
 * 
 * @author dibyendumajumdar
 * @since 7 Oct 2007
 */
public class TableDefinitionImpl implements Storable, TableDefinition {

	final Logger log;
	final MessageCatalog mcat;
	final PlatformObjects po;
	
	/**
	 * Database to which this table definition belongs to.
	 */
    final Database database;
    
    /**
     * Container ID for the table.
     */
    int containerId;
    
    /**
     * The name of the Table Container.
     */
    String name;
    
    /**
     * The row type for the table. The row type is used to create row
     * instances.
     */
    TypeDescriptor[] rowType;
    
    /**
     * List of indexes associated with the table.
     */
    ArrayList<IndexDefinition> indexes = new ArrayList<IndexDefinition>();

    TableDefinitionImpl(PlatformObjects po, Database database, ByteBuffer bb) {
    	this.po = po;
    	this.log = po.getLogger();
    	this.mcat = po.getMessageCatalog();
		this.database = database;
		containerId = bb.getInt();
		ByteString s = new ByteString(bb);
		name = s.toString();
		rowType = database.getTypeFactory().retrieve(bb);
		int n = bb.getShort();
		indexes = new ArrayList<IndexDefinition>();
		for (int i = 0; i < n; i++) {
			IndexDefinitionImpl idx = new IndexDefinitionImpl(po, this, bb);
			indexes.add(idx);
		}
	}

    TableDefinitionImpl(PlatformObjects po, Database database, int containerId, String name,
            TypeDescriptor[] rowType) {
    	this.po = po;
    	this.log = po.getLogger();
    	this.mcat = po.getMessageCatalog();
        this.database = database;
        this.containerId = containerId;
        this.name = name;
        this.rowType = rowType;
    }

    /* (non-Javadoc)
	 * @see org.simpledbm.database.TableDefinition#addIndex(int, java.lang.String, int[], boolean, boolean)
	 */
    public void addIndex(int containerId, String name, int[] columns,
            boolean primary, boolean unique) {
        if (!primary && indexes.size() == 0) {
        	log.error(getClass().getName(), "addIndex", mcat.getMessage("ED0012"));
        	throw new DatabaseException(mcat.getMessage("ED0012"));
        }
        indexes.add(new IndexDefinitionImpl(po, this, containerId, name, columns, primary, unique));
    }

    /* (non-Javadoc)
	 * @see org.simpledbm.database.TableDefinition#getDatabase()
	 */
    public Database getDatabase() {
        return database;
    }

    /* (non-Javadoc)
	 * @see org.simpledbm.database.TableDefinition#getContainerId()
	 */
    public int getContainerId() {
        return containerId;
    }

    /* (non-Javadoc)
	 * @see org.simpledbm.database.TableDefinition#getName()
	 */
    public String getName() {
        return name;
    }

    /* (non-Javadoc)
	 * @see org.simpledbm.database.TableDefinition#getRowType()
	 */
    public TypeDescriptor[] getRowType() {
        return rowType;
    }

    /* (non-Javadoc)
	 * @see org.simpledbm.database.TableDefinition#getIndexes()
	 */
    public ArrayList<IndexDefinition> getIndexes() {
        return indexes;
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.st.Storable#getStoredLength()
     */
    public int getStoredLength() {
        int n = 0;
        ByteString s = new ByteString(name);
        n += TypeSize.INTEGER;
        n += s.getStoredLength();
        n += database.getTypeFactory().getStoredLength(rowType);
        n += TypeSize.SHORT;
        for (int i = 0; i < indexes.size(); i++) {
            n += indexes.get(i).getStoredLength();
        }
        return n;
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.st.Storable#store(java.nio.ByteBuffer)
     */
    public void store(ByteBuffer bb) {
        bb.putInt(containerId);
        ByteString s = new ByteString(name);
        s.store(bb);
        database.getTypeFactory().store(rowType, bb);
        bb.putShort((short) indexes.size());
        for (int i = 0; i < indexes.size(); i++) {
            IndexDefinition idx = indexes.get(i);
            idx.store(bb);
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + containerId;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final TableDefinitionImpl other = (TableDefinitionImpl) obj;
        if (containerId != other.containerId) {
            return false;
        }
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
            return false;
        }
        return true;
    }

    /* (non-Javadoc)
	 * @see org.simpledbm.database.TableDefinition#getRow()
	 */
    public Row getRow() {
        RowFactory rowFactory = database.getRowFactory();
        return rowFactory.newRow(containerId);
    }
    
    /* (non-Javadoc)
	 * @see org.simpledbm.database.TableDefinition#getRow()
	 */
    public Row getRow(ByteBuffer bb) {
        RowFactory rowFactory = database.getRowFactory();
        return rowFactory.newRow(containerId, bb);
    }

    /* (non-Javadoc)
	 * @see org.simpledbm.database.TableDefinition#getIndexRow(org.simpledbm.database.IndexDefinition, org.simpledbm.typesystem.api.Row)
	 */
    public Row getIndexRow(IndexDefinition index, Row tableRow) {
        Row indexRow = index.getRow();
        for (int i = 0; i < index.getColumns().length; i++) {
            indexRow.set(i, tableRow, index.getColumns()[i]);
        }
        return indexRow;
    }

    /* (non-Javadoc)
     * @see org.simpledbm.database.api.TableDefinition#getNumberOfIndexes()
     */
    public int getNumberOfIndexes() {
    	return indexes.size();
    }
    
	/* (non-Javadoc)
	 * @see org.simpledbm.database.api.TableDefinition#getIndex(int)
	 */
	public IndexDefinition getIndex(int indexNo) {
		if (indexNo < 0 || indexNo  >= indexes.size()) {
			// FIXME
			// log error
			throw new DatabaseException();
		}
		return indexes.get(indexNo);
	}

	/* (non-Javadoc)
	 * @see org.simpledbm.database.api.TableDefinition#getIndexRow(int, org.simpledbm.typesystem.api.Row)
	 */
	public Row getIndexRow(int indexNo, Row tableRow) {
		return getIndexRow(getIndex(indexNo), tableRow);
	}

	public StringBuilder appendTo(StringBuilder sb) {
		sb.append(newline).
			append("TableDefinition(containerId=").append(containerId).
			append(", name='").append(name).
			append("', column definitions = {").append(newline);
		for (int i = 0; i < rowType.length; i++) {
			sb.append(TAB).append("column[").append(i).append("] type=");
			if (i == rowType.length - 1) {
				sb.append(rowType[i]);
			}
			else {
				sb.append(rowType[i]).append(", ").append(newline);
			}
		}
		sb.append("}").append(newline);
		sb.append("index definitions = {").append(newline);
		for (int i = 0; i < indexes.size(); i++) {
			sb.append(TAB).append("index[").append(i).append("] = ");
			if (i == indexes.size() - 1) {
				sb.append(indexes.get(i));
			}
			else {
				sb.append(indexes.get(i)).append(", ").append(newline);
			}
		}
		sb.append("})").append(newline);
		return sb;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		return appendTo(sb).toString();
	}
}
