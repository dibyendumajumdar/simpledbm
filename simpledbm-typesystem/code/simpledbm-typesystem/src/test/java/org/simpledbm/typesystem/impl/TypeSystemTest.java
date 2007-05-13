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
 * Created on: Nov 16, 2005
 * Author: Dibyendu Majumdar
 */
package org.simpledbm.typesystem.impl;

import java.nio.ByteBuffer;

import junit.framework.TestCase;

import org.simpledbm.typesystem.api.FieldFactory;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.RowFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;

public class TypeSystemTest extends TestCase {

    public static void main(String[] args) {
    }

    public TypeSystemTest(String arg0) {
        super(arg0);
    }

    public void testCase1() throws Exception {
        FieldFactory fieldFactory = new DefaultFieldFactory();
        RowFactory rowFactory = new GenericRowFactory(fieldFactory);
        TypeDescriptor[] rowtype1 = new TypeDescriptor[] {
            new IntegerType(), new VarcharType(10)
        };
        rowFactory.registerRowType(1, rowtype1);
        Row row = rowFactory.newRow(1);
        assertEquals(row.getNumberOfFields(), 2);
        System.err.println("Number of fields in row = " + row.getNumberOfFields());
        row.get(0).setInt(5432);
        row.get(1).setInt(2345);
        assertEquals(row.get(0).getInt(), 5432);
        assertEquals(row.get(1).getInt(), 2345);
        System.err.println("Row contents = " + row);
        Row rowClone = row.cloneMe();
        System.err.println("Cloned Row contents = " + rowClone);
        ByteBuffer bb = ByteBuffer.allocate(row.getStoredLength());
        row.store(bb);
        bb.flip();
        Row row1 = rowFactory.newRow(1);
        row1.retrieve(bb);
        assertTrue(row.compareTo(row1) == 0);
        System.err.println("Row1 contents = " + row1);
        Row row3 = row.cloneMe();
        assertTrue(row.compareTo(row3) == 0);
        row.get(0).setString("9876");
        assertTrue(row.compareTo(row3) > 0);
        System.err.println("Row contents = " + row);
        System.err.println("Row3 contents = " + row3);
        System.err.println("Comparing row and row1 = " + row.compareTo(row1));
        System.err.println("Comparing row and row3 = " + row.compareTo(row3));
        System.err.println("Comparing row3 and row = " + row3.compareTo(row));
        System.err.println("Comparing row3 and row1 = " + row3.compareTo(row1));
        row.parseString("300,hello world");
        assertTrue(row.get(0).getInt() == 300);
        assertTrue(row.get(1).getString().equals("hello"));
        System.err.println("Row contents after parse string = " + row);
        row.get(1).setString("a");
        row3.get(1).setString("ab");
        assertTrue(row.get(1).compareTo(row3.get(1)) < 0);
        row3.get(1).setNull();
        assertTrue(row.get(1).compareTo(row3.get(1)) > 0);
        assertTrue(row3.get(1).isNull());
        row.get(0).setPositiveInfinity();
        row3.get(0).setInt(Integer.MAX_VALUE);
        assertTrue(row.get(0).compareTo(row3.get(0)) > 0);
        assertTrue(!row.get(0).isNull());
        row.get(0).setNegativeInfinity();
        row3.get(0).setInt(Integer.MIN_VALUE);
        assertTrue(row.get(0).compareTo(row3.get(0)) < 0);
        assertTrue(!row.get(0).isNull());
    }
}
