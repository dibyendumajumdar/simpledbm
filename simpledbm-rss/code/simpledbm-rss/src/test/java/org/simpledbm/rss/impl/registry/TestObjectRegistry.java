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
package org.simpledbm.rss.impl.registry;

import java.nio.ByteBuffer;
import java.util.Properties;

import org.simpledbm.junit.BaseTestCase;
import org.simpledbm.rss.api.registry.ObjectCreationException;
import org.simpledbm.rss.api.registry.ObjectFactory;
import org.simpledbm.rss.api.registry.ObjectRegistry;
import org.simpledbm.rss.api.registry.Storable;

/**
 * Test cases for the Object Registry module.
 * 
 * @author Dibyendu Majumdar
 * @since 21-Aug-2005
 */
public class TestObjectRegistry extends BaseTestCase {
	
	static class MyStorable implements Storable {

		final int value;
		
		public MyStorable(int i) {
			this.value = i;
		}
		
		public MyStorable(ByteBuffer buf) {
			buf.getShort();
			this.value = buf.getInt();
		}
		
		public int getStoredLength() {
			return 6;
		}

		public void retrieve(ByteBuffer bb) {
			throw new UnsupportedOperationException();
		}

		public void store(ByteBuffer bb) {
			bb.putInt(3);
			bb.putInt(value);
		}
		
		static class MyStorableFactory implements ObjectFactory {
			public Class<?> getType() {
				return MyStorable.class;
			}
			public Object newInstance() {
				return new MyStorable(0);
			}
			public Object newInstance(ByteBuffer buf) {
				return new MyStorable(buf);
			}
		}
	}

	static class StringFactory implements ObjectFactory {
		public Class<?> getType() {
			return String.class;
		}
		public Object newInstance() {
			return new String();
		}
		public Object newInstance(ByteBuffer buf) {
			buf.getShort();
			return new String();
		}
	}

	static class IntegerFactory implements ObjectFactory {
		public Class<?> getType() {
			return Integer.class;
		}
		public Object newInstance() {
			return new Integer(0);
		}
		public Object newInstance(ByteBuffer buf) {
			buf.getShort();
			return new Integer(0);
		}
	}

    public TestObjectRegistry(String arg0) {
        super(arg0);
    }

    public void testRegistry() throws Exception {
        ObjectRegistry factory = new ObjectRegistryImpl(new Properties());
        Integer i = new Integer(55);
        factory.registerObjectFactory(1, new StringFactory());
        factory.registerSingleton(2, i);
        factory.registerSingleton(2, i);
        assertTrue(i == factory.getSingleton(2));
        assertTrue(i == factory.getSingleton(2));
//        Object s = factory.getInstance(1);
//        assertFalse(s == null);
//        assertTrue(s instanceof String);
//        Object s1 = factory.getInstance(1);
//        assertFalse(s1 == null);
//        assertTrue(s1 instanceof String);
//        assertFalse(s == s1);
        Object s = null;
        try {
            s = factory.getSingleton(5);
            fail();
        } catch (ObjectCreationException e) {
            assertTrue(e.getMessage().startsWith("SIMPLEDBM-ER0006"));
        }
        Integer x = new Integer(10);
        try {
            factory.registerSingleton(2, x);
            fail();
        } catch (ObjectCreationException e) {
            assertTrue(e.getMessage().startsWith("SIMPLEDBM-ER0004"));
        }
        assertTrue(i == factory.getSingleton(2));
        factory.registerObjectFactory(1, new StringFactory());
        try {
            factory.registerObjectFactory(1, new IntegerFactory());
            fail();
        } catch (ObjectCreationException e) {
            assertTrue(e.getMessage().startsWith("SIMPLEDBM-ER0002"));
        }
        
        factory.registerObjectFactory(3, new MyStorable.MyStorableFactory());
        ByteBuffer buf = ByteBuffer.allocate(6);
        buf.putShort((short) 3);
        buf.putInt(311566);
        buf.flip();
        MyStorable o = (MyStorable) factory.getInstance(buf);
        assertEquals(311566, o.value);

        buf = ByteBuffer.allocate(6);
        buf.putShort((short) 3);
        buf.putInt(322575);
        buf.flip();
        o = (MyStorable) factory.getInstance(buf);
        assertEquals(322575, o.value);

    }

}
