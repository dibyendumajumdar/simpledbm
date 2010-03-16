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
package org.simpledbm.common.impl.registry;

import java.nio.ByteBuffer;
import java.util.Properties;

import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.registry.ObjectCreationException;
import org.simpledbm.common.api.registry.ObjectFactory;
import org.simpledbm.common.api.registry.ObjectRegistry;
import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.impl.platform.PlatformImpl;
import org.simpledbm.common.impl.registry.ObjectRegistryImpl;
import org.simpledbm.junit.BaseTestCase;

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
        Properties properties = new Properties();
        properties.setProperty("logging.properties.file",
                "classpath:simpledbm.logging.properties");
        properties.setProperty("logging.properties.type", "log4j");
        final Platform platform = new PlatformImpl(properties);
        ObjectRegistry factory = new ObjectRegistryImpl(platform, properties);
        Integer i = new Integer(55);
        factory.registerObjectFactory(1, new StringFactory());
        factory.registerSingleton(2, i);
        factory.registerSingleton(2, i);
        assertTrue(i == factory.getSingleton(2));
        assertTrue(i == factory.getSingleton(2));
        // Object s = factory.getInstance(1);
        // assertFalse(s == null);
        // assertTrue(s instanceof String);
        // Object s1 = factory.getInstance(1);
        // assertFalse(s1 == null);
        // assertTrue(s1 instanceof String);
        // assertFalse(s == s1);
        @SuppressWarnings("unused")
        Object s = null;
        try {
            s = factory.getSingleton(5);
            fail();
        } catch (ObjectCreationException e) {
            assertTrue(e.getErrorCode() == 6);
        }
        Integer x = new Integer(10);
        try {
            factory.registerSingleton(2, x);
            fail();
        } catch (ObjectCreationException e) {
            assertTrue(e.getErrorCode() == 4);
        }
        assertTrue(i == factory.getSingleton(2));
        factory.registerObjectFactory(1, new StringFactory());
        try {
            factory.registerObjectFactory(1, new IntegerFactory());
            fail();
        } catch (ObjectCreationException e) {
            assertTrue(e.getErrorCode() == 2);
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
