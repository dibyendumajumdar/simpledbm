package org.simpledbm.rss.main;

import java.util.Properties;

import junit.framework.TestCase;

import org.simpledbm.rss.api.exception.RSSException;
import org.simpledbm.rss.api.tuple.TupleContainer;
import org.simpledbm.rss.api.tx.IsolationMode;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.rss.util.ByteString;

public class TestServer extends TestCase {

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
            server.getSpaceManager().createContainer(
            		trx, "test.db", 1, 2, 20,
            		server.getSlottedPageManager().getPageType());
            trx.commit();			
		}
		finally {
			server.shutdown();
		}
		
		server = new Server(properties);
		server.start();
		try {
      Transaction trx = server.getTransactionManager().begin(IsolationMode.SERIALIZABLE);
      TupleContainer container = server.getTupleManager().getTupleContainer(trx, 1);
      container.insert(trx, new ByteString("Hello World!"));
      trx.commit();			
		}
		finally {
			server.shutdown();
		}
		
	}
	
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
		}
		catch (RSSException e) {
			assertTrue(e.getMessage().startsWith("SIMPLEDBM-EV0005"));
			server.shutdown();
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
		}
		catch (RSSException e) {
			assertTrue(e.getMessage().startsWith("SIMPLEDBM-EV0003"));
			return;
		}
		fail("Unexpected result - server startup should have failed");
	}

}
