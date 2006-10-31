package org.simpledbm.rss.util.logging;

import org.simpledbm.rss.util.logging.Logger;

import junit.framework.TestCase;

public class TestLogging extends TestCase {

	public TestLogging() {
		super();
	}

	public TestLogging(String arg0) {
		super(arg0);
	}

	public void testCase1() throws Exception {
		Logger.configure("logging.properties");
		Logger logger = Logger.getLogger(getClass().getName());
		assertFalse(logger.isDebugEnabled());
		logger.debug("test", "test", "This should not appear");
		logger.enableDebug();
		assertTrue(logger.isDebugEnabled());
		logger.debug("test", "test", "This is the first message that should appear");
		logger.disableDebug();
		assertFalse(logger.isDebugEnabled());
		logger.debug("test", "test", "This should not appear");
		logger.info("test", "test", "This is the second message that should appear");
	}
	
}
