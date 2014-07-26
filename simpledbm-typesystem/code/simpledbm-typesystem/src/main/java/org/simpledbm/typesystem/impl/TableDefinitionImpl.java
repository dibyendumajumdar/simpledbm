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
package org.simpledbm.typesystem.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.simpledbm.common.api.exception.ExceptionHandler;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.util.ByteString;
import org.simpledbm.common.util.TypeSize;
import org.simpledbm.common.util.logging.Logger;
import org.simpledbm.common.util.mcat.Message;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.common.util.mcat.MessageType;
import org.simpledbm.typesystem.api.IndexDefinition;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.RowFactory;
import org.simpledbm.typesystem.api.TableDefinition;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.api.TypeException;
import org.simpledbm.typesystem.api.TypeFactory;

/**
 * Encapsulates a table definition and provides methods to work with table and
 * indexes associated with the table.
 * 
 * @author dibyendumajumdar
 * @since 7 Oct 2007
 */
public class TableDefinitionImpl implements Storable, TableDefinition {

    final Logger log;
    final ExceptionHandler exceptionHandler;
    final PlatformObjects po;

    static final Message m_EY0012 = new Message('T', 'Y', MessageType.ERROR,
            12, "The first index for the table must be the primary index");
    static final Message m_EY0014 = new Message('T', 'Y', MessageType.ERROR,
            14, "Index {0} is not defined");

    /**
     * Database to which this table definition belongs to.
     */
    //    final Database database;

    /**
     * Row Factory responsible for generating and manipulating rows.
     */
    final RowFactory rowFactory;

    /**
     * Type Factory responsible for defining types.
     */
    final TypeFactory typeFactory;

    /**
     * Container ID for the table.
     */
    int containerId;

    /**
     * The name of the Table Container.
     */
    String name;

    /**
     * The row type for the table. The row type is used to create row instances.
     */
    TypeDescriptor[] rowType;

    /**
     * List of indexes associated with the table.
     */
    ArrayList<IndexDefinition> indexes = new ArrayList<IndexDefinition>();

    public TableDefinitionImpl(PlatformObjects po, TypeFactory typeFactory,
            RowFactory rowFactory, ByteBuffer bb) {
        this.po = po;
        this.log = po.getLogger();
        this.exceptionHandler = po.getExceptionHandler();
        //		this.database = database;
        this.typeFactory = typeFactory;
        this.rowFactory = rowFactory;
        containerId = bb.getInt();
        ByteString s = new ByteString(bb);
        name = s.toString();
        rowType = typeFactory.retrieve(bb);
        int n = bb.getShort();
        indexes = new ArrayList<IndexDefinition>();
        for (int i = 0; i < n; i++) {
            IndexDefinitionImpl idx = new IndexDefinitionImpl(po, this, bb);
            indexes.add(idx);
        }
    }

    public TableDefinitionImpl(PlatformObjects po, TypeFactory typeFactory,
            RowFactory rowFactory, int containerId, String name,
            TypeDescriptor[] rowType) {
        this.po = po;
        this.log = po.getLogger();
        this.exceptionHandler = po.getExceptionHandler();
        this.typeFactory = typeFactory;
        this.rowFactory = rowFactory;
        //        this.database = database;
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
            exceptionHandler.errorThrow(getClass(), "addIndex",
                    new TypeException(new MessageInstance(m_EY0012)));
        }
        indexes.add(new IndexDefinitionImpl(po, this, containerId, name,
                columns, primary, unique));
    }

    /* (non-Javadoc)
     * @see org.simpledbm.database.TableDefinition#getDatabase()
     */
    //    public Database getDatabase() {
    //        return database;
    //    }

    public RowFactory getRowFactory() {
        return rowFactory;
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
        n += typeFactory.getStoredLength(rowType);
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
        typeFactory.store(rowType, bb);
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
        //        RowFactory rowFactory = database.getRowFactory();
        return rowFactory.newRow(containerId);
    }

    /* (non-Javadoc)
     * @see org.simpledbm.database.TableDefinition#getRow()
     */
    public Row getRow(ByteBuffer bb) {
        //        RowFactory rowFactory = database.getRowFactory();
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
        if (indexNo < 0 || indexNo >= indexes.size()) {
            exceptionHandler.errorThrow(getClass(), "getIndex",
                    new TypeException(new MessageInstance(m_EY0014, indexNo)));
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
        sb.append(newline).append("TableDefinition(containerId=").append(
                containerId).append(", name='").append(name).append(
                "', column definitions = {").append(newline);
        for (int i = 0; i < rowType.length; i++) {
            sb.append(TAB).append("column[").append(i).append("] type=");
            if (i == rowType.length - 1) {
                sb.append(rowType[i]);
            } else {
                sb.append(rowType[i]).append(", ").append(newline);
            }
        }
        sb.append("}").append(newline);
        sb.append("index definitions = {").append(newline);
        for (int i = 0; i < indexes.size(); i++) {
            sb.append(TAB).append("index[").append(i).append("] = ");
            if (i == indexes.size() - 1) {
                sb.append(indexes.get(i));
            } else {
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
