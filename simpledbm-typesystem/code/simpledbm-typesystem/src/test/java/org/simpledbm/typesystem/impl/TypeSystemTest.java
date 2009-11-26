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
/*
 * Created on: Nov 16, 2005
 * Author: Dibyendu Majumdar
 */
package org.simpledbm.typesystem.impl;

import java.nio.ByteBuffer;
import java.util.Date;

import junit.framework.TestCase;

import org.simpledbm.typesystem.api.DictionaryCache;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.RowFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.api.TypeFactory;
import org.simpledbm.typesystem.api.TypeSystemFactory;

public class TypeSystemTest extends TestCase {

    public static void main(String[] args) {
    }

    public TypeSystemTest(String arg0) {
        super(arg0);
    }

    public void testRowFactory() throws Exception {
    	DictionaryCache dictionaryCache = new SimpleDictionaryCache();
        TypeFactory fieldFactory = TypeSystemFactory.getDefaultTypeFactory();
        RowFactory rowFactory = TypeSystemFactory.getDefaultRowFactory(fieldFactory, dictionaryCache);
        TypeDescriptor[] rowtype1 = new TypeDescriptor[] {
            fieldFactory.getIntegerType(), fieldFactory.getVarcharType(10)
        };
        for (TypeDescriptor td: rowtype1) {
            System.err.println(td);
        }
        System.err.println(fieldFactory.getDateTimeType());
        System.err.println(fieldFactory.getNumberType(2));
        dictionaryCache.registerRowType(1, rowtype1);
        Row row = rowFactory.newRow(1);
        assertEquals(row.getNumberOfColumns(), 2);
        System.err.println("Number of fields in row = " + row.getNumberOfColumns());
        row.setInt(0, 5432);
        row.setInt(1, 2345);
        assertEquals(row.getInt(0), 5432);
        assertEquals(row.getInt(1), 2345);
        System.err.println("Row contents = " + row);
        Row rowClone = row.cloneMe();
        System.err.println("Cloned Row contents = " + rowClone);
        ByteBuffer bb = ByteBuffer.allocate(row.getStoredLength());
        row.store(bb);
        bb.flip();
        Row row1 = rowFactory.newRow(1, bb);
//        row1.retrieve(bb);
        System.err.println("Row1 contents = " + row1);
        assertTrue(row.compareTo(row1) == 0);
        Row row3 = row.cloneMe();
        assertTrue(row.compareTo(row3) == 0);
        row.setString(0, "9876");
        assertTrue(row.compareTo(row3) > 0);
        System.err.println("Row contents = " + row);
        System.err.println("Row3 contents = " + row3);
        System.err.println("Comparing row and row1 = " + row.compareTo(row1));
        System.err.println("Comparing row and row3 = " + row.compareTo(row3));
        System.err.println("Comparing row3 and row = " + row3.compareTo(row));
        System.err.println("Comparing row3 and row1 = " + row3.compareTo(row1));
        ((GenericRow)row).parseString("300,hello world");
        assertTrue(row.getInt(0) == 300);
        assertTrue(row.getString(1).equals("hello worl"));
        System.err.println("Row contents after parse string = " + row);
        row.setString(1, "a");
        row3.setString(1, "ab");
        assertTrue(row.getString(1).compareTo(row3.getString(1)) < 0);
        row3.setNull(1);
        assertTrue(row.compareTo(row3, 1) > 0);
        assertTrue(row3.isNull(1));
        row.setPositiveInfinity(0);
        row3.setInt(0, Integer.MAX_VALUE);
        assertTrue(row.compareTo(row3, 0) > 0);
        assertTrue(!row.isNull(0));
        row.setNegativeInfinity(0);
        row3.setInt(0, Integer.MIN_VALUE);
        assertTrue(row.compareTo(row3, 0) < 0);
        assertTrue(!row.isNull(0));
    }
    
    public void testBigDecimal() {
        TypeFactory fieldFactory = TypeSystemFactory.getDefaultTypeFactory();
        TypeDescriptor type = fieldFactory.getNumberType(2);
        NumberValue f1 = (NumberValue) fieldFactory.getInstance(type);
        f1.setString("780.919");
        assertEquals("780.92", f1.toString());
        NumberValue f2 = (NumberValue) fieldFactory.getInstance(type);
        f2.setInt(781);
        assertEquals("781.00", f2.toString());
        assertTrue(f2.compareTo(f1) > 0);
        NumberValue f3 = (NumberValue) f1.cloneMe();
        assertEquals(f1, f3);
    }
    
    public void testDateTime() {
        TypeFactory fieldFactory = TypeSystemFactory.getDefaultTypeFactory();
        TypeDescriptor type = fieldFactory.getDateTimeType();
        DateTimeValue f1 = (DateTimeValue) fieldFactory.getInstance(type);
        assertTrue(f1.isNull());
        f1.setString("14-Dec-1989 00:00:00 +0000");
        assertEquals(f1.getString(), "14-Dec-1989 00:00:00 +0000");
        Date d = new Date();
        f1.setDate(d);
        assertEquals(d.getTime(), f1.getLong());
        ByteBuffer bb = ByteBuffer.allocate(f1.getStoredLength());
        f1.store(bb);
        bb.flip();
        DateTimeValue f2 = (DateTimeValue) fieldFactory.getInstance(type, bb);
//        f2.retrieve(bb);
        assertEquals(f1, f2);
        assertEquals(f2.getLong(), d.getTime());
    }
    
    public void testVarchar() {
        TypeFactory fieldFactory = TypeSystemFactory.getDefaultTypeFactory();
        final TypeDescriptor type = fieldFactory.getVarcharType(10);
    	VarcharValue f1 = (VarcharValue) fieldFactory.getInstance(type);
    	assertTrue(f1.isNull());
    	assertFalse(f1.isNegativeInfinity());
    	assertFalse(f1.isPositiveInfinity());
    	assertFalse(f1.isValue());
    	VarcharValue f2 = (VarcharValue) f1.cloneMe();
    	assertEquals(f1, f2);
    	assertTrue(f2.isNull());
    	assertFalse(f2.isNegativeInfinity());
    	assertFalse(f2.isPositiveInfinity());
    	assertFalse(f2.isValue());
    	f1.setString("");
    	assertFalse(f1.isNull());
    	assertFalse(f1.isNegativeInfinity());
    	assertFalse(f1.isPositiveInfinity());
    	assertTrue(f1.isValue());
    	assertTrue(!f1.equals(f2));
    	int n = f1.getStoredLength();
    	assertEquals(3, n);
    	ByteBuffer bb = ByteBuffer.allocate(n);
    	f1.store(bb);
    	bb.flip();
    	f2 = (VarcharValue) fieldFactory.getInstance(type, bb);
    	assertEquals(f1, f2);
    	assertTrue("".equals(f1.toString()));
    	assertTrue("".equals(f2.toString()));
    	f1.setString("abcdefghijkl");
    	assertFalse(f1.isNull());
    	assertFalse(f1.isNegativeInfinity());
    	assertFalse(f1.isPositiveInfinity());
    	assertTrue(f1.isValue());
    	assertTrue(!f1.equals(f2));
    	assertTrue("abcdefghij".equals(f1.toString()));
    	assertFalse("abcdefghijkl".equals(f1.toString()));
    	f2.setString("abcdefghik");
    	assertTrue(f1.compareTo(f2) < 0);
    	assertTrue(f2.compareTo(f1) > 0);
    	VarcharValue f3 = (VarcharValue) f1.cloneMe();
    	f3.setPositiveInfinity();
    	assertTrue(f1.compareTo(f3) < 0);
    	assertTrue(f2.compareTo(f3) < 0);
    	assertFalse(f3.isNegativeInfinity());
    	assertFalse(f3.isNull());
    	assertFalse(f3.isValue());
    	assertEquals("+infinity", f3.toString());
    	VarcharValue f4 = (VarcharValue) fieldFactory.getInstance(type);
    	f4.setPositiveInfinity();
    	assertTrue(f3.equals(f4));
    	assertFalse(f4.isNegativeInfinity());
    	assertFalse(f4.isNull());
    	assertFalse(f4.isValue());
    	assertEquals("+infinity", f4.toString());
    	assertEquals(f3, f4);
    }
    
    public void testVarbinary() {
        TypeFactory fieldFactory = TypeSystemFactory.getDefaultTypeFactory();
        final TypeDescriptor type = fieldFactory.getVarbinaryType(10);
    	VarbinaryValue f1 = (VarbinaryValue) fieldFactory.getInstance(type);
    	f1.setString("68656c6c");
    	assertTrue(f1.getString().equals("68656C6C"));
    	f1.setString("00010203040506070809");
    	assertTrue(f1.getString().equals("00010203040506070809"));
    	ByteBuffer bb = ByteBuffer.allocate(7);
    	f1.setString("68656c6c");
    	f1.store(bb);
    	bb.flip();
    	f1 = (VarbinaryValue) fieldFactory.getInstance(type, bb);
    	assertTrue(f1.getString().equals("68656C6C"));
    }
    
    public void testStorage() {
        TypeFactory fieldFactory = TypeSystemFactory.getDefaultTypeFactory();
        TypeDescriptor[] rowtype1 = new TypeDescriptor[] {
            fieldFactory.getIntegerType(), fieldFactory.getVarcharType(10),
            fieldFactory.getDateTimeType(), fieldFactory.getNumberType(),
            fieldFactory.getLongType(), fieldFactory.getVarbinaryType(10)
        };
    	int n = fieldFactory.getStoredLength(rowtype1);
    	ByteBuffer bb = ByteBuffer.allocate(n);
    	fieldFactory.store(rowtype1, bb);
    	bb.flip();
    	TypeDescriptor[] rowtype2 = fieldFactory.retrieve(bb);
    	assertEquals(rowtype1.length, rowtype2.length);
    	for (int i = 0; i < rowtype1.length; i++) {
    		assertTrue(rowtype1[i] != rowtype2[i]);
    		assertEquals(rowtype1[i], rowtype2[i]);
    		assertEquals(rowtype1[i].hashCode(), rowtype2[i].hashCode());
    	}
    }
}
