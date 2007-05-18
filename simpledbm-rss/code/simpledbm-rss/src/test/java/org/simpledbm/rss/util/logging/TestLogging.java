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
		logger.debug("test", "test", "This is the first message that should appear");
		logger.disableDebug();
		assertFalse(logger.isDebugEnabled());
		logger.debug("test", "test", "This should not appear");
		logger.info("test", "test", "This is the second message that should appear");
		logger.info(this.getClass().getName(), "testCase1", "This message has two arguments: [{0}] and [{1}]", "one", "two");
	}
	
}
