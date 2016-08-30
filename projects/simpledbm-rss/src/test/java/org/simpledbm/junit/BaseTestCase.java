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
package org.simpledbm.junit;

import java.util.Properties;
import java.util.Vector;

import junit.framework.TestCase;

import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.impl.platform.PlatformImpl;

public abstract class BaseTestCase extends TestCase {

    Vector<ThreadFailure> threadFailureExceptions;

    protected Platform platform;

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
        properties.setProperty("logging.properties.file",
                "classpath:simpledbm.logging.properties");
        properties.setProperty("logging.properties.type", "log4j");
        platform = new PlatformImpl(properties);
    }

    @Override
    protected void tearDown() throws Exception {
        threadFailureExceptions = null;
        platform.shutdown();
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
