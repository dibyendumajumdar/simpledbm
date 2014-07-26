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
/*
 * Created on: Nov 16, 2005
 * Author: Dibyendu Majumdar
 */
package org.simpledbm.typesystem.impl;

import java.nio.ByteBuffer;
import java.util.Date;

import org.simpledbm.junit.BaseTestCase;
import org.simpledbm.typesystem.api.DictionaryCache;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.RowFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.api.TypeFactory;
import org.simpledbm.typesystem.api.TypeSystemFactory;

public class TypeSystemTest extends BaseTestCase {

    public static void main(String[] args) {
    }

    public TypeSystemTest(String arg0) {
        super(arg0);
    }

    public void testRowFactory() throws Exception {
        DictionaryCache dictionaryCache = new SimpleDictionaryCache();
        TypeSystemFactory typeSystemFactory = new TypeSystemFactoryImpl(
                properties, platform
                        .getPlatformObjects(TypeSystemFactory.LOGGER_NAME));
        TypeFactory fieldFactory = typeSystemFactory.getDefaultTypeFactory();
        RowFactory rowFactory = typeSystemFactory.getDefaultRowFactory(
                fieldFactory, dictionaryCache);
        TypeDescriptor[] rowtype1 = new TypeDescriptor[] {
                fieldFactory.getIntegerType(), fieldFactory.getVarcharType(10) };
        for (TypeDescriptor td : rowtype1) {
            System.err.println(td);
        }
        System.err.println(fieldFactory.getDateTimeType());
        System.err.println(fieldFactory.getNumberType(2));
        dictionaryCache.registerRowType(1, rowtype1);
        Row row = rowFactory.newRow(1);
        assertEquals(row.getNumberOfColumns(), 2);
        System.err.println("Number of fields in row = "
                + row.getNumberOfColumns());
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
        ((GenericRow) row).parseString("300,hello world");
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
        TypeSystemFactory typeSystemFactory = new TypeSystemFactoryImpl(
                properties, platform
                        .getPlatformObjects(TypeSystemFactory.LOGGER_NAME));
        TypeFactory fieldFactory = typeSystemFactory.getDefaultTypeFactory();
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
        TypeSystemFactory typeSystemFactory = new TypeSystemFactoryImpl(
                properties, platform
                        .getPlatformObjects(TypeSystemFactory.LOGGER_NAME));
        TypeFactory fieldFactory = typeSystemFactory.getDefaultTypeFactory();
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
        TypeSystemFactory typeSystemFactory = new TypeSystemFactoryImpl(
                properties, platform
                        .getPlatformObjects(TypeSystemFactory.LOGGER_NAME));
        TypeFactory fieldFactory = typeSystemFactory.getDefaultTypeFactory();
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
        TypeSystemFactory typeSystemFactory = new TypeSystemFactoryImpl(
                properties, platform
                        .getPlatformObjects(TypeSystemFactory.LOGGER_NAME));
        TypeFactory fieldFactory = typeSystemFactory.getDefaultTypeFactory();
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
        TypeSystemFactory typeSystemFactory = new TypeSystemFactoryImpl(
                properties, platform
                        .getPlatformObjects(TypeSystemFactory.LOGGER_NAME));
        TypeFactory fieldFactory = typeSystemFactory.getDefaultTypeFactory();
        TypeDescriptor[] rowtype1 = new TypeDescriptor[] {
                fieldFactory.getIntegerType(), fieldFactory.getVarcharType(10),
                fieldFactory.getDateTimeType(), fieldFactory.getNumberType(),
                fieldFactory.getLongType(), fieldFactory.getVarbinaryType(10) };
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
