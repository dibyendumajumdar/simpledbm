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
package org.simpledbm.typesystem.impl;

import java.nio.ByteBuffer;

import org.simpledbm.rss.api.im.IndexKey;
import org.simpledbm.typesystem.api.DataValue;
import org.simpledbm.typesystem.api.TypeFactory;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.TypeDescriptor;

public class GenericRow implements Row, IndexKey, Cloneable {

    DataValue[] fields;
    
    // final FieldFactory fieldFactory;
    
    public GenericRow(TypeFactory fieldFactory, TypeDescriptor[] rowTypeDesc) {
        // this.fieldFactory = fieldFactory;
        fields = new DataValue[rowTypeDesc.length];
        for (int i = 0; i < rowTypeDesc.length; i++) {
            fields[i] = fieldFactory.getInstance(rowTypeDesc[i]); 
        }
    }
    
	public int getNumberOfColumns() {
		return fields.length;
	}

	public DataValue getColumnValue(int i) {
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
            }
            else {
                break;
            }
        }
	}

	public void retrieve(ByteBuffer bb) {
        for (int i = 0; i < fields.length; i++) {
            fields[i].retrieve(bb);
        }
	}

	public void store(ByteBuffer bb) {
        for (int i = 0; i < fields.length; i++) {
            fields[i].store(bb);
        }
	}

	public int getStoredLength() {
        int n = 0;
        for (DataValue f: fields) {
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
	
    @Override
    public Object clone() throws CloneNotSupportedException {
        GenericRow row = (GenericRow) super.clone();
        row.fields = new DataValue[fields.length];
        for (int i = 0; i < fields.length; i++) {
            row.fields[i] = (DataValue) fields[i].cloneMe();
        }
        return row;
    }

    public final Row cloneMe() {
    	try {
    		return (Row) clone();
    	}
    	catch (CloneNotSupportedException e) {
    		throw new RuntimeException(e);
    	}
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            if (i != 0) {
                sb.append(", [");
            }
            else {
                sb.append("[");
            }
            sb.append(fields[i].toString());
            sb.append("]");
        }
        return sb.toString();
    }

}
