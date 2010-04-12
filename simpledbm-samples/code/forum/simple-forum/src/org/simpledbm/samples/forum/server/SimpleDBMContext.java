package org.simpledbm.samples.forum.server;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.simpledbm.network.client.api.SessionManager;

public class SimpleDBMContext {

    final static int TABLE_SEQUENCE = 7;

    final SessionManager sm;

    SimpleDBMContext(Properties properties) {
        System.err.println("Connecting to SimpleDBM server");
        sm = SessionManager.getSessionManager(properties, "localhost", 8000,
                (int) TimeUnit.MILLISECONDS.convert(5 * 60, TimeUnit.SECONDS));
    }

    void destroy() {
        System.err.println("Terminating connection to SimpleDBM server");
        if (sm != null) {
            sm.getConnection().close();
        }
    }

    SessionManager getSessionManager() {
        return sm;
    }

}
