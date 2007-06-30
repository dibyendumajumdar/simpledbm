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
package org.simpledbm.rss.impl.registry;

import junit.framework.TestCase;

import org.simpledbm.rss.api.registry.ObjectCreationException;
import org.simpledbm.rss.api.registry.ObjectRegistry;

/**
 * Test cases for the Object Registry module.
 * 
 * @author Dibyendu Majumdar
 * @since 21-Aug-2005
 */
public class TestObjectRegistry extends TestCase {

    public TestObjectRegistry(String arg0) {
        super(arg0);
    }

    public void testRegistry() throws Exception {
        ObjectRegistry factory = new ObjectRegistryImpl();
        Integer i = new Integer(55);
        factory.registerType(1, String.class.getName());
        factory.registerSingleton(2, i);
        factory.registerSingleton(2, i);
        assertTrue(i == factory.getInstance(2));
        assertTrue(i == factory.getInstance(2));
        Object s = factory.getInstance(1);
        assertFalse(s == null);
        assertTrue(s instanceof String);
        Object s1 = factory.getInstance(1);
        assertFalse(s1 == null);
        assertTrue(s1 instanceof String);
        assertFalse(s == s1);
        try {
            s = factory.getInstance(5);
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
        assertTrue(i == factory.getInstance(2));
        factory.registerType(1, String.class.getName());
        try {
            factory.registerType(1, Integer.class.getName());
            fail();
        } catch (ObjectCreationException e) {
            assertTrue(e.getMessage().startsWith("SIMPLEDBM-ER0002"));
        }
    }

}
