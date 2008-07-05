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
package org.simpledbm.rss.main;

import java.util.Properties;

import org.simpledbm.junit.BaseTestCase;
import org.simpledbm.rss.api.exception.RSSException;
import org.simpledbm.rss.api.tuple.TupleContainer;
import org.simpledbm.rss.api.tuple.TupleInserter;
import org.simpledbm.rss.api.tx.IsolationMode;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.rss.util.ByteString;

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
        } catch (RSSException e) {
            assertTrue(e.getMessage().startsWith("SIMPLEDBM-EV0005"));
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
        } catch (RSSException e) {
            assertTrue(e.getMessage().startsWith("SIMPLEDBM-EV0003"));
            Server.drop(properties);
            return;
        }
        fail("Unexpected result - server startup should have failed");
    }

}
