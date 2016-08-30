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
package org.simpledbm.rss.main;

import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.simpledbm.common.api.exception.SimpleDBMException;
import org.simpledbm.common.api.tx.IsolationMode;
import org.simpledbm.common.util.ByteString;
import org.simpledbm.junit.BaseTestCase;
import org.simpledbm.rss.api.tuple.TupleContainer;
import org.simpledbm.rss.api.tuple.TupleInserter;
import org.simpledbm.rss.api.tx.Transaction;

public class TestServer extends BaseTestCase {

    public TestServer(String arg0) {
        super(arg0);
    }

    public void testCase1() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("log.ctl.1", "log/control1/ctl.a");
        properties.setProperty("log.ctl.2", "log/control2/ctl.b");
        properties.setProperty("log.groups.1.path", "log/current");
        properties.setProperty("log.archive.path", "log/archive");
        properties.setProperty("log.group.files", "3");
        properties.setProperty("log.file.size", "65536");
        properties.setProperty("log.buffer.size", "65536");
        properties.setProperty("log.buffer.limit", "4");
        properties.setProperty("log.flush.interval", "30");
        properties.setProperty("storage.basePath", "testdata/TestServer");

        Server.create(properties);

        Server server = new Server(properties);
        server.start();
        server.shutdown();

        Server.drop(properties);
    }

    public void testCase2() throws Exception {

        Properties properties = new Properties();
        properties.setProperty("log.ctl.1", "log/control1/ctl.a");
        properties.setProperty("log.ctl.2", "log/control2/ctl.b");
        properties.setProperty("log.groups.1.path", "log/current");
        properties.setProperty("log.archive.path", "log/archive");
        properties.setProperty("log.group.files", "3");
        properties.setProperty("log.file.size", "65536");
        properties.setProperty("log.buffer.size", "65536");
        properties.setProperty("log.buffer.limit", "64");
        properties.setProperty("log.flush.interval", "30");
        properties.setProperty("storage.basePath", "testdata/TestServer2");

        Server.create(properties);

        Server server = new Server(properties);
        server.start();
        try {
            Transaction trx = server.begin(IsolationMode.READ_COMMITTED);
            server.createTupleContainer(trx, "test.db", 1, 20);
            trx.commit();
        } finally {
            server.shutdown();
        }

        server = new Server(properties);
        server.start();
        try {
            Transaction trx = server.begin(IsolationMode.SERIALIZABLE);
            TupleContainer container = server.getTupleContainer(trx, 1);

            TupleInserter inserter = container.insert(trx, new ByteString(
                    "Hello World!"));
            inserter.completeInsert();
            trx.commit();
        } finally {
            server.shutdown();
        }

        Server.drop(properties);
    }

    // test case disabled because it fails on Mac due to different
    // locking behaviour
    public void testCase3() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("log.ctl.1", "log/control1/ctl.a");
        properties.setProperty("log.ctl.2", "log/control2/ctl.b");
        properties.setProperty("log.groups.1.path", "log/current");
        properties.setProperty("log.archive.path", "log/archive");
        properties.setProperty("log.group.files", "3");
        properties.setProperty("log.file.size", "65536");
        properties.setProperty("log.buffer.size", "65536");
        properties.setProperty("log.buffer.limit", "4");
        properties.setProperty("log.flush.interval", "30");
        properties.setProperty("storage.basePath", "testdata/TestServer");

        Server.create(properties);

        Server server = new Server(properties);
        server.start();

        Server server2 = new Server(properties);
        try {
            server2.start();
        } catch (SimpleDBMException e) {
            assertTrue(e.getErrorCode() == 5);
            server.shutdown();
            Server.drop(properties);
            return;
        }
        fail("Unexpected result - server2 startup should have failed");
    }

    public void testCase4() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("log.ctl.1", "log/control1/ctl.a");
        properties.setProperty("log.ctl.2", "log/control2/ctl.b");
        properties.setProperty("log.groups.1.path", "log/current");
        properties.setProperty("log.archive.path", "log/archive");
        properties.setProperty("log.group.files", "3");
        properties.setProperty("log.file.size", "65536");
        properties.setProperty("log.buffer.size", "65536");
        properties.setProperty("log.buffer.limit", "4");
        properties.setProperty("log.flush.interval", "30");
        properties.setProperty("storage.basePath", "testdata/TestServer");

        Server.create(properties);

        Server server = new Server(properties);
        server.start();
        server.shutdown();

        try {
            server.start();
        } catch (SimpleDBMException e) {
            assertTrue(e.getErrorCode() == 3);
            Server.drop(properties);
            return;
        }
        fail("Unexpected result - server startup should have failed");
    }

    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTest(new TestServer("testCase1"));
        suite.addTest(new TestServer("testCase2"));
        suite.addTest(new TestServer("testCase3"));
        suite.addTest(new TestServer("testCase4"));
        return suite;
    }


}
