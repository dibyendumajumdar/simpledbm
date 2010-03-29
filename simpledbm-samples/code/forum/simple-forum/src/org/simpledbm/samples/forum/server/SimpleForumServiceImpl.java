package org.simpledbm.samples.forum.server;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;

import org.simpledbm.samples.forum.client.SimpleForumService;
import org.simpledbm.samples.forum.client.Topic;
import org.simpledbm.samples.forum.client.TopicList;
import org.simpledbm.samples.forum.shared.FieldVerifier;

import com.google.gwt.user.server.rpc.RemoteServiceServlet;

/**
 * The server side implementation of the RPC service.
 */
@SuppressWarnings("serial")
public class SimpleForumServiceImpl extends RemoteServiceServlet implements
        SimpleForumService {

    @Override
    public void destroy() {
        super.destroy();

        this.log("destroy called");
    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        System.err.println("init called");
        this.log("init called");
    }

    public String greetServer(String input) throws IllegalArgumentException {
        // Verify that the input is valid.
        if (!FieldVerifier.isValidName(input)) {
            // If the input is not valid, throw an IllegalArgumentException back
            // to
            // the client.
            throw new IllegalArgumentException(
                    "Name must be at least 4 characters long");
        }

        String serverInfo = getServletContext().getServerInfo();
        String userAgent = getThreadLocalRequest().getHeader("User-Agent");
        return "Hello, " + input + "!<br><br>I am running " + serverInfo
                + ".<br><br>It looks like you are using:<br>" + userAgent;
    }

    public TopicList getTopics(String forumName) {

        if (forumName.equals("Nikon")) {
            Topic[] topics = new Topic[5];
            for (int i = 0; i < topics.length; i++) {
                topics[i] = new Topic();
                topics[i].setTitle("Nikon topic " + i);
            }
            TopicList topicList = new TopicList();
            topicList.setTopics(topics);
            return topicList;
        } else {
            Topic[] topics = new Topic[5];
            for (int i = 0; i < topics.length; i++) {
                topics[i] = new Topic();
                topics[i].setTitle("Zeiss topic " + i);
            }
            TopicList topicList = new TopicList();
            topicList.setTopics(topics);
            return topicList;
        }
    }

    //    static final class SimpleDBMContext {
    //
    //        final SessionManager sm;
    //
    //        SimpleDBMContext() {
    //            Properties properties = new Properties();
    //            properties
    //                    .setProperty(
    //                            "logging.properties",
    //                            "/Users/dibyendumajumdar/simpledbm-samples-workspace/simple-forum-db/config/simpledbm.logging.properties");
    //            sm = SessionManager.getSessionManager(properties, "localhost",
    //                    8000, 30);
    //        }
    //
    //        void destroy() {
    //        // Session session = sm.openSession();
    //        // session.close();
    //            if (sm != null) {
    //                sm.getConnection().close();
    //            }
    //        }
    //    }
}
