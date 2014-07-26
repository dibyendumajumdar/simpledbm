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
package org.simpledbm.common.impl.registry;

import java.nio.ByteBuffer;
import java.util.Properties;

import org.simpledbm.common.api.registry.ObjectCreationException;
import org.simpledbm.common.api.registry.ObjectFactory;
import org.simpledbm.common.api.registry.ObjectRegistry;
import org.simpledbm.common.api.registry.Storable;
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
//        final Platform platform = new PlatformImpl(properties);
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
