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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Date;

import org.simpledbm.common.api.key.IndexKey;
import org.simpledbm.typesystem.api.DataValue;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.api.TypeFactory;

public class GenericRow implements Row, IndexKey {

    DataValue[] fields;

    GenericRow(GenericRow row) {
        this.fields = new DataValue[row.fields.length];
        for (int i = 0; i < fields.length; i++) {
            this.fields[i] = row.fields[i].cloneMe();
        }
    }

    public GenericRow(TypeFactory fieldFactory, TypeDescriptor[] rowTypeDesc) {
        fields = new DataValue[rowTypeDesc.length];
        for (int i = 0; i < rowTypeDesc.length; i++) {
            fields[i] = fieldFactory.getInstance(rowTypeDesc[i]);
        }
    }

    public GenericRow(TypeFactory fieldFactory, TypeDescriptor[] rowTypeDesc,
            ByteBuffer bb) {
        fields = new DataValue[rowTypeDesc.length];
        for (int i = 0; i < rowTypeDesc.length; i++) {
            fields[i] = fieldFactory.getInstance(rowTypeDesc[i], bb);
        }
    }

    public int getNumberOfColumns() {
        return fields.length;
    }

    public DataValue getColumnValue(int i) {
        if (i < 0 || i >= fields.length) {
            throw new IllegalArgumentException();
        }
        return fields[i];
    }

    public void setColumnValue(int i, DataValue field) {
        fields[i] = field;
    }

    public void parseString(String string) {
        String[] items = string.split(",");
        for (int i = 0; i < items.length; i++) {
            if (i < fields.length) {
                fields[i].setString(items[i]);
            } else {
                break;
            }
        }
    }

    public void store(ByteBuffer bb) {
        for (int i = 0; i < fields.length; i++) {
            fields[i].store(bb);
        }
    }

    public int getStoredLength() {
        int n = 0;
        for (DataValue f : fields) {
            n += f.getStoredLength();
        }
        return n;
    }

    public int compareTo(IndexKey o) {
        if (o == null || !(o instanceof GenericRow)) {
            return -1;
        }
        GenericRow other = (GenericRow) o;
        for (int i = 0; i < fields.length; i++) {
            int result = fields[i].compareTo(other.getColumnValue(i));
            if (result != 0) {
                return result > 0 ? 1 : -1;
            }
        }
        return 0;
    }

    public boolean equals(Object o) {
        if (o == null || !(o instanceof GenericRow)) {
            return false;
        }
        GenericRow other = (GenericRow) o;
        return compareTo(other) == 0;
    }

    public final Row cloneMe() {
        return new GenericRow(this);
    }

    public IndexKey cloneIndexKey() {
        return cloneMe();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        return appendTo(sb).toString();
    }

    public StringBuilder appendTo(StringBuilder sb) {
        for (int i = 0; i < fields.length; i++) {
            if (i != 0) {
                sb.append(", [");
            } else {
                sb.append("[");
            }
            fields[i].appendTo(sb);
            sb.append("]");
        }
        return sb;
    }

    public BigDecimal getBigDecimal(int column) {
        return getColumnValue(column).getBigDecimal();
    }

    public BigInteger getBigInteger(int column) {
        return getColumnValue(column).getBigInteger();
    }

    public byte[] getBytes(int column) {
        return getColumnValue(column).getBytes();
    }

    public Date getDate(int column) {
        return getColumnValue(column).getDate();
    }

    public int getInt(int column) {
        return getColumnValue(column).getInt();
    }

    public long getLong(int column) {
        return getColumnValue(column).getLong();
    }

    public String getString(int column) {
        return getColumnValue(column).getString();
    }

    public boolean isNegativeInfinity(int column) {
        return getColumnValue(column).isNegativeInfinity();
    }

    public boolean isNull(int column) {
        return getColumnValue(column).isNull();
    }

    public boolean isPositiveInfinity(int column) {
        return getColumnValue(column).isPositiveInfinity();
    }

    public boolean isValue(int column) {
        return getColumnValue(column).isValue();
    }

    public void setBigDecimal(int column, BigDecimal d) {
        getColumnValue(column).setBigDecimal(d);
    }

    public void setBigInteger(int column, BigInteger i) {
        getColumnValue(column).setBigInteger(i);
    }

    public void setBytes(int column, byte[] bytes) {
        getColumnValue(column).setBytes(bytes);
    }

    public void setDate(int column, Date date) {
        getColumnValue(column).setDate(date);
    }

    public void setInt(int column, Integer integer) {
        getColumnValue(column).setInt(integer);
    }

    public void setLong(int column, long l) {
        getColumnValue(column).setLong(l);
    }

    public void setNegativeInfinity(int column) {
        getColumnValue(column).setNegativeInfinity();
    }

    public void setNull(int column) {
        getColumnValue(column).setNull();
    }

    public void setPositiveInfinity(int column) {
        getColumnValue(column).setPositiveInfinity();
    }

    public void setString(int column, String string) {
        getColumnValue(column).setString(string);
    }

    public int compareTo(Row row, int column) {
        return getColumnValue(column).compareTo(
                ((GenericRow) row).getColumnValue(column));
    }

    public void set(int column, Row sourceRow, int sourceColumn) {
        setColumnValue(column, ((GenericRow) sourceRow).getColumnValue(
                sourceColumn).cloneMe());
    }
}
