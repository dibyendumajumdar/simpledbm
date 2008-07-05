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
package org.simpledbm.rss.util.logging;

import org.simpledbm.junit.BaseTestCase;

public class TestLogging extends BaseTestCase {

    public TestLogging() {
        super();
    }

    public TestLogging(String arg0) {
        super(arg0);
    }

    public void testCase1() throws Exception {
        Logger logger = Logger.getLogger(getClass().getName());
        
        assertFalse(logger.isDebugEnabled());
        logger.debug("test", "test", "This should not appear");
        logger.enableDebug();
        assertTrue(logger.isDebugEnabled());
        logger.debug(
            "test",
            "test",
            "This is the first message that should appear");
        logger.disableDebug();
        assertFalse(logger.isDebugEnabled());
        logger.debug("test", "test", "This should not appear");
        logger.info(
            "test",
            "test",
            "This is the second message that should appear");
//        logger.info(
//            this.getClass().getName(),
//            "testCase1",
//            "This message has two arguments: [{0}] and [{1}]",
//            "one",
//            "two");
    }

}
