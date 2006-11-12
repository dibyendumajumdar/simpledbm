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
package org.simpledbm.typesystem.impl;

import java.nio.ByteBuffer;

import org.simpledbm.rss.api.im.IndexKey;
import org.simpledbm.typesystem.api.Field;
import org.simpledbm.typesystem.api.FieldFactory;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.TypeDescriptor;

public class IndexRow implements Row, IndexKey {

    Field[] fields;
    
    // final FieldFactory fieldFactory;
    
    public IndexRow(FieldFactory fieldFactory, TypeDescriptor[] rowTypeDesc) {
        // this.fieldFactory = fieldFactory;
        fields = new Field[rowTypeDesc.length];
        for (int i = 0; i < rowTypeDesc.length; i++) {
            fields[i] = fieldFactory.getInstance(rowTypeDesc[i]); 
        }
    }
    
	public int getNumberOfFields() {
		return fields.length;
	}

	public Field get(int i) {
		return fields[i];
	}

	public void set(int i, Field field) {
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
            // fields[i] = fieldFactory.retrieve(bb);
        }
	}

	public void store(ByteBuffer bb) {
        for (int i = 0; i < fields.length; i++) {
            fields[i].store(bb);
            // fieldFactory.store(bb, fields[i]);
        }
	}

	public int getStoredLength() {
        int n = 0;
        for (Field f: fields) {
            n += f.getStoredLength();
            // n += fieldFactory.getStoredLength(f);
        }
        return n;
	}

	public int compareTo(IndexKey o) {
        if (o == null || !(o instanceof IndexRow)) {
            return -1;
        }
        IndexRow other = (IndexRow) o;
        for (int i = 0; i < fields.length; i++) {
            int result = fields[i].compareTo(other.get(i));
            if (result != 0) {
                return result > 0 ? 1 : -1;
            }
        }
		return 0;
	}

    @Override
    public Object clone() throws CloneNotSupportedException {
        IndexRow row = (IndexRow) super.clone();
        row.fields = new Field[fields.length];
        for (int i = 0; i < fields.length; i++) {
            row.fields[i] = (Field) fields[i].clone();
        }
        return row;
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
