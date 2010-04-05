package org.simpledbm.samples.forum.server;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;

import org.simpledbm.samples.forum.client.Forum;
import org.simpledbm.samples.forum.client.Post;
import org.simpledbm.samples.forum.client.SimpleForumService;
import org.simpledbm.samples.forum.client.Topic;
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
    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        System.err.println("init called");
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

    public Topic[] getTopics(String forumName) {
        Topic[] topics = new Topic[15];
        for (int i = 0; i < topics.length; i++) {
            topics[i] = new Topic();
            topics[i].setTopicId(forumName + "-" + i);
            topics[i].setTitle(forumName + " topic " + i);
        }
        return topics;
    }

    public Forum[] getForums() {
        Forum[] forums = new Forum[10];
        forums[0] = new Forum("Nikon", "Nikon Discussions");
        forums[1] = new Forum("Canon", "Canon Discussions");
        forums[2] = new Forum("Leica", "Leica Discussions");
        forums[3] = new Forum("Zeiss", "Zeiss Discussions");
        forums[4] = new Forum("Ricoh", "Ricoh Discussions");
        forums[5] = new Forum("Sony", "Sony Discussions");
        forums[6] = new Forum("Olympus", "Olympus Discussions");
        forums[7] = new Forum("Panasonic", "Panasonic Discussions");
        forums[8] = new Forum("Pentax", "Pentax Discussions");
        forums[9] = new Forum("Kodak", "Kodak Discussions");
        return forums;
    }

    public Post[] getPosts(String topicId) {
        Post[] posts = new Post[10];
        for (int i = 0; i < 10; i++) {
            posts[i] = new Post();
            posts[i].setContent(topicId + " content post # " + i);
        }
        return posts;
    }
    
    public void savePost(Post post) {
        throw new RuntimeException("Unable to save post " + post);
    }

    public void saveTopic(Topic topic, Post post) {
        throw new RuntimeException("Unable to save topic");
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
