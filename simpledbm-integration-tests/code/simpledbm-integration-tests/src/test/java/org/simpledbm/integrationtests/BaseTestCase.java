package org.simpledbm.integrationtests;

import java.util.Properties;
import java.util.Vector;

import org.simpledbm.rss.util.logging.Logger;

import junit.framework.TestCase;

public class BaseTestCase extends TestCase {

    Vector<ThreadFailure> threadFailureExceptions;

    public BaseTestCase() {
    }

    public BaseTestCase(String arg0) {
        super(arg0);
    }

    public final void setThreadFailed(Thread thread, Throwable exception) {
        threadFailureExceptions.add(new ThreadFailure(thread, exception));
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        threadFailureExceptions = new Vector<ThreadFailure>();
        Properties properties = new Properties();
        properties.setProperty("logging.properties.file", "classpath:simpledbm.logging.properties");
        properties.setProperty("logging.properties.type", "log4j");
        Logger.configure(properties);
    }

    @Override
    protected void tearDown() throws Exception {
        threadFailureExceptions = null;
        super.tearDown();
    }

    public final void checkThreadFailures() throws Exception {
        for (ThreadFailure tf : threadFailureExceptions) {
            System.err.println("Thread [" + tf.threadName + " failed");
            tf.exception.printStackTrace();
        }
        if (threadFailureExceptions.size() > 0) {
            fail(threadFailureExceptions.size()
                    + " number of threads have failed the test");
        }
    }

    final static class ThreadFailure {
        Throwable exception;
        String threadName;

        public ThreadFailure(Thread thread, Throwable exception) {
            this.threadName = thread.getName();
            this.exception = exception;
        }
    }	
	
}
