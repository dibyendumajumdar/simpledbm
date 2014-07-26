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

import org.simpledbm.common.api.exception.ExceptionHandler;
import org.simpledbm.common.api.key.IndexKey;
import org.simpledbm.common.api.key.IndexKeyFactory;
import org.simpledbm.common.api.platform.PlatformObjects;
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

/**
 * An implementation of IndexDefinition.
 * 
 * @author dibyendumajumdar
 */
public class IndexDefinitionImpl implements IndexDefinition {

    final Logger log;
    final ExceptionHandler exceptionHandler;

    static final Message m_EY0010 = new Message('T', 'Y', MessageType.ERROR,
            10, "An index must have at least one column");
    static final Message m_EY0011 = new Message('T', 'Y', MessageType.ERROR,
            11, "The column {0} does not exist in table definition for {1}");

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

    IndexDefinitionImpl(PlatformObjects po, TableDefinition table, ByteBuffer bb) {
        this.log = po.getLogger();
        this.exceptionHandler = po.getExceptionHandler();
        this.table = table;
        containerId = bb.getInt();
        ByteString s = new ByteString(bb);
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
    }

    IndexDefinitionImpl(PlatformObjects po, TableDefinition table,
            int containerId, String name, int columns[], boolean primary,
            boolean unique) {
        this.log = po.getLogger();
        this.exceptionHandler = po.getExceptionHandler();
        this.table = table;
        if (columns.length == 0) {
            exceptionHandler.errorThrow(getClass(),
                    "IndexDefinitionImpl", new TypeException(
                            new MessageInstance(m_EY0010)));
        }
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
                exceptionHandler.errorThrow(getClass(),
                        "IndexDefinitionImpl", new TypeException(
                                new MessageInstance(m_EY0011, columns[i], table
                                        .getName())));
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
        RowFactory rowFactory = table.getRowFactory();
        return rowFactory.newRow(containerId);
    }

    /**
     * Create a row with values that are less than any other row in the index.
     * 
     * @param containerId ID of the container
     * @return Appropriate row type
     */
    IndexKey makeMinRow(int containerId) {
        IndexKeyFactory rowFactory = table.getRowFactory();
        return rowFactory.minIndexKey(containerId);
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.st.Storable#getStoredLength()
     */
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

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.st.Storable#store(java.nio.ByteBuffer)
     */
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

    public StringBuilder appendTo(StringBuilder sb) {
        sb.append("IndexDefinition(containerId=").append(containerId).append(
                ", name=").append(name).append(", unique=").append(unique)
                .append(", primary=").append(primary).append(", columns={");
        for (int i = 0; i < columns.length; i++) {
            if (i == columns.length - 1) {
                sb.append(columns[i]);
            } else {
                sb.append(columns[i]).append(", ");
            }
        }
        sb.append("})");
        return sb;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        return appendTo(sb).toString();
    }
}
