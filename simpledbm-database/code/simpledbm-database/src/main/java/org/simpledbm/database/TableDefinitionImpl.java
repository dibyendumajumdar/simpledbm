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
import java.util.ArrayList;

import org.simpledbm.database.api.Database;
import org.simpledbm.database.api.IndexDefinition;
import org.simpledbm.database.api.TableDefinition;
import org.simpledbm.rss.api.st.Storable;
import org.simpledbm.rss.util.ByteString;
import org.simpledbm.rss.util.TypeSize;
import org.simpledbm.typesystem.api.Field;
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

    Database database;
    int containerId;
    String name;
    TypeDescriptor[] rowType;
    ArrayList<IndexDefinition> indexes = new ArrayList<IndexDefinition>();

    TableDefinitionImpl(Database database) {
        this.database = database;
    }

    TableDefinitionImpl(Database database, int containerId, String name,
            TypeDescriptor[] rowType) {
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
            throw new IllegalArgumentException(
                    "First index must be the primary");
        }
        indexes.add(new IndexDefinitionImpl(this, containerId, name, columns, primary, unique));
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

    public int getStoredLength() {
        int n = 0;
        ByteString s = new ByteString(name);
        n += TypeSize.INTEGER;
        n += s.getStoredLength();
        n += database.getFieldFactory().getStoredLength(rowType);
        n += TypeSize.SHORT;
        for (int i = 0; i < indexes.size(); i++) {
            n += indexes.get(i).getStoredLength();
        }
        return n;
    }

    public void retrieve(ByteBuffer bb) {
        containerId = bb.getInt();
        ByteString s = new ByteString();
        s.retrieve(bb);
        name = s.toString();
        rowType = database.getFieldFactory().retrieve(bb);
        int n = bb.getShort();
        indexes = new ArrayList<IndexDefinition>();
        for (int i = 0; i < n; i++) {
            IndexDefinitionImpl idx = new IndexDefinitionImpl(this);
            idx.retrieve(bb);
        }
//        if (!database.tables.contains(this)) {
//            database.getRowFactory().registerRowType(containerId, rowType);
//            database.tables.add(this);
//        }
    }

    public void store(ByteBuffer bb) {
        bb.putInt(containerId);
        ByteString s = new ByteString(name);
        s.store(bb);
        database.getFieldFactory().store(rowType, bb);
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
	 * @see org.simpledbm.database.TableDefinition#getIndexRow(org.simpledbm.database.IndexDefinition, org.simpledbm.typesystem.api.Row)
	 */
    public Row getIndexRow(IndexDefinition index, Row tableRow) {
        Row indexRow = index.getRow();
        for (int i = 0; i < index.getColumns().length; i++) {
            indexRow.set(i, (Field) tableRow.get(index.getColumns()[i]).cloneMe());
        }
        return indexRow;
    }
}
