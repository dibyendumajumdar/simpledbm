package org.simpledbm.samples.forum.server;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.simpledbm.network.client.api.SessionManager;

public class SimpleDBMContext {

    final static int TABLE_SEQUENCE = 7;
    final static int TABLE_TOPIC = 3;
    final static int TABLE_FORUM = 1;
    final static int TABLE_POST = 5;

    final static int TOPIC_FORUM_NAME = 0;
    final static int TOPIC_TOPIC_ID = 1;
    final static int TOPIC_TITLE = 2;
    final static int TOPIC_NUM_POSTS = 3;
    final static int TOPIC_STARTED_BY = 4;
    final static int TOPIC_LAST_UPDATED_BY = 5;
    final static int TOPIC_LAST_UPDATED_ON = 6;
    
    final static int FORUM_NAME = 0;
    final static int FORUM_DESCRIPTION = 1;
    
    final static int POST_FORUM_NAME = 0;
    final static int POST_TOPIC_ID = 1;
    final static int POST_POST_ID = 2;
    final static int POST_AUTHOR = 3;
    final static int POST_DATE_TIME = 4;
    final static int POST_CONTENT = 5;
    
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
