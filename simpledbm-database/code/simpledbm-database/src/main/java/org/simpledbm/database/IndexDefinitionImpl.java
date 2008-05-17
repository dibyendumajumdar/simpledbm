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
package org.simpledbm.database;

import java.nio.ByteBuffer;

import org.simpledbm.database.api.IndexDefinition;
import org.simpledbm.database.api.TableDefinition;
import org.simpledbm.rss.api.exception.RSSException;
import org.simpledbm.rss.api.im.IndexKey;
import org.simpledbm.rss.api.im.IndexKeyFactory;
import org.simpledbm.rss.api.st.Storable;
import org.simpledbm.rss.util.ByteString;
import org.simpledbm.rss.util.TypeSize;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.RowFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;

public class IndexDefinitionImpl implements IndexDefinition {

    /**
     * Table to which this index belongs.
     */
    TableDefinition table;
    /**
     * Container ID for the index.
     */
    int containerId;
    /**
     * Name of the index.
     */
    String name;
    /**
     * Columns from the table that will be part of the index.
     */
    int columns[];
    /**
     * A row descriptor for the index, derived from the table columns.
     */
    TypeDescriptor[] rowType;
    /**
     * Is this a primary index?
     */
    boolean primary;
    /**
     * Is this a unique index?
     */
    boolean unique;

    IndexDefinitionImpl(TableDefinition table) {
        this.table = table;
    }

    public IndexDefinitionImpl(TableDefinition table, int containerId, String name,
            int columns[], boolean primary, boolean unique) {
        this.table = table;
        this.containerId = containerId;
        this.name = name;
        this.columns = columns;
        this.primary = primary;
        if (primary) {
            this.unique = true;
        } else {
            this.unique = unique;
        }

        rowType = new TypeDescriptor[columns.length];
        for (int i = 0; i < columns.length; i++) {
            if (columns[i] >= table.getRowType().length || columns[i] < 0) {
                throw new RSSException("Error: The spcified column of the index does not exist");
            }
            rowType[i] = table.getRowType()[columns[i]];
        }
    }

    /* (non-Javadoc)
	 * @see org.simpledbm.database.IndexDefinition#getTable()
	 */
    public TableDefinition getTable() {
        return table;
    }

    /* (non-Javadoc)
	 * @see org.simpledbm.database.IndexDefinition#getContainerId()
	 */
    public int getContainerId() {
        return containerId;
    }

    /* (non-Javadoc)
	 * @see org.simpledbm.database.IndexDefinition#getName()
	 */
    public String getName() {
        return name;
    }

    /* (non-Javadoc)
	 * @see org.simpledbm.database.IndexDefinition#getColumns()
	 */
    public int[] getColumns() {
        return columns;
    }

    /* (non-Javadoc)
	 * @see org.simpledbm.database.IndexDefinition#getRowType()
	 */
    public TypeDescriptor[] getRowType() {
        return rowType;
    }

    /* (non-Javadoc)
	 * @see org.simpledbm.database.IndexDefinition#isPrimary()
	 */
    public boolean isPrimary() {
        return primary;
    }

    /* (non-Javadoc)
	 * @see org.simpledbm.database.IndexDefinition#isUnique()
	 */
    public boolean isUnique() {
        return unique;
    }

    /* (non-Javadoc)
	 * @see org.simpledbm.database.IndexDefinition#getRow()
	 */
    public Row getRow() {
        RowFactory rowFactory = table.getDatabase().getRowFactory();
        return rowFactory.newRow(containerId);
    }

    /**
     * Create a row with values that are less than any other row in the index.
     * 
     * @param containerId
     *            ID of the container
     * @return Appropriate row type
     */
    IndexKey makeMinRow(int containerId) {
        IndexKeyFactory rowFactory = table.getDatabase().getRowFactory();
        return rowFactory.minIndexKey(containerId);
    }

    public int getStoredLength() {
        ByteString s = new ByteString(name);
        int n = 0;

        n += s.getStoredLength();
        n += TypeSize.INTEGER;
        n += TypeSize.BYTE * 2;
        n += TypeSize.SHORT;
        for (int i = 0; i < columns.length; i++) {
            n += TypeSize.SHORT;
        }

        return n;
    }

    public void retrieve(ByteBuffer bb) {
        ByteString s = new ByteString();
        containerId = bb.getInt();
        s.retrieve(bb);
        name = s.toString();
        byte b = bb.get();
        if (b == 1) {
            primary = true;
        } else {
            primary = false;
        }
        b = bb.get();
        if (b == 1 || primary) {
            unique = true;
        } else {
            unique = false;
        }
        int n = bb.getShort();
        columns = new int[n];
        for (int i = 0; i < n; i++) {
            columns[i] = bb.getShort();
        }
        rowType = new TypeDescriptor[columns.length];
        for (int i = 0; i < columns.length; i++) {
            rowType[i] = table.getRowType()[columns[i]];
        }
        if (!table.getIndexes().contains(this)) {
            table.getDatabase().getRowFactory().registerRowType(containerId, rowType);
            table.getIndexes().add(this);
        }
    }

    public void store(ByteBuffer bb) {
        bb.putInt(containerId);
        ByteString s = new ByteString(name);
        s.store(bb);
        if (primary) {
            bb.put((byte) 1);
        } else {
            bb.put((byte) 0);
        }
        if (unique || primary) {
            bb.put((byte) 1);
        } else {
            bb.put((byte) 0);
        }
        bb.putShort((short) columns.length);
        for (int i = 0; i < columns.length; i++) {
            bb.putShort((short) columns[i]);
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
        final IndexDefinitionImpl other = (IndexDefinitionImpl) obj;
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
}
