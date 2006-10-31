package org.simpledbm.rss.main;

import java.util.Properties;

import org.simpledbm.rss.impl.st.FileStorageContainerFactory;

import junit.framework.TestCase;

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
		properties.setProperty(FileStorageContainerFactory.BASE_PATH, "/temp/test");

		Server.create(properties);
	}

}
