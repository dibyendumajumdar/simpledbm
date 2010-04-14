package org.simpledbm.samples.forum.server;

import java.util.ArrayList;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;

import org.simpledbm.common.api.tx.IsolationMode;
import org.simpledbm.network.client.api.Session;
import org.simpledbm.network.client.api.SessionManager;
import org.simpledbm.network.client.api.Table;
import org.simpledbm.network.client.api.TableScan;
import org.simpledbm.samples.forum.client.Forum;
import org.simpledbm.samples.forum.client.Post;
import org.simpledbm.samples.forum.client.SimpleForumService;
import org.simpledbm.samples.forum.client.Topic;
import org.simpledbm.samples.forum.shared.FieldVerifier;
import org.simpledbm.typesystem.api.Row;

import com.google.gwt.user.server.rpc.RemoteServiceServlet;

/**
 * The server side implementation of the RPC service.
 */
@SuppressWarnings("serial")
public class SimpleForumServiceImpl extends RemoteServiceServlet implements
        SimpleForumService {

    private SimpleDBMContext sdbmContext;

    @Override
    public void destroy() {
        sdbmContext = null;
        super.destroy();
    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        sdbmContext = (SimpleDBMContext) config.getServletContext()
                .getAttribute("org.simpledbm.samples.forum.sdbmContext");
        if (sdbmContext == null) {
            throw new ServletException(
                    "Unexpected error: SimpleDBMContext does not exist");
        }
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
        SessionManager sm = sdbmContext.getSessionManager();
        Session session = sm.openSession();
        ArrayList<Topic> topics = new ArrayList<Topic>();
        try {
            session.startTransaction(IsolationMode.READ_COMMITTED);
            try {
                Table table = session.getTable(SimpleDBMContext.TABLE_TOPIC);
                Row row = table.getRow();
                row.setString(0, forumName);
                TableScan scan = table.openScan(0, row, false);
                try {
                    row = scan.fetchNext();
                    while (row != null) {
                        Topic t = new Topic();
                        t.setForumName(row
                                .getString(SimpleDBMContext.TOPIC_FORUM_NAME));
                        t.setTopicId(row
                                .getLong(SimpleDBMContext.TOPIC_TOPIC_ID));
                        //                        t.setNumPosts(row
                        //                                .getInt(SimpleDBMContext.TOPIC_NUM_POSTS));
                        t.setStartedBy(row
                                .getString(SimpleDBMContext.TOPIC_STARTED_BY));
                        //                        t
                        //                                .setLastPoster(row
                        //                                        .getString(SimpleDBMContext.TOPIC_LAST_UPDATED_BY));
                        //            t.setUpdatedOn(row.getDate(SimpleDBMContext.TOPIC_LAST_UPDATED_ON));
                        topics.add(t);
                        row = scan.fetchNext();
                    }
                } finally {
                    scan.close();
                }
            } finally {
                session.commit();
            }
        } finally {
            session.close();
        }
        return topics.toArray(new Topic[0]);
    }

    public Forum[] getForums() {
        SessionManager sm = sdbmContext.getSessionManager();
        Session session = sm.openSession();
        ArrayList<Forum> topics = new ArrayList<Forum>();
        try {
            session.startTransaction(IsolationMode.READ_COMMITTED);
            try {
                Table table = session.getTable(SimpleDBMContext.TABLE_FORUM);
                TableScan scan = table.openScan(0, null, false);
                try {
                    Row row = scan.fetchNext();
                    while (row != null) {
                        Forum t = new Forum(row
                                .getString(SimpleDBMContext.FORUM_NAME), row
                                .getString(SimpleDBMContext.FORUM_DESCRIPTION));
                        topics.add(t);
                        row = scan.fetchNext();
                    }
                } finally {
                    scan.close();
                }
            } finally {
                session.commit();
            }
        } finally {
            session.close();
        }
        return topics.toArray(new Forum[0]);
    }

    public Post[] getPosts(String forumName, long topicId) {
        SessionManager sm = sdbmContext.getSessionManager();
        Session session = sm.openSession();
        ArrayList<Post> posts = new ArrayList<Post>();
        try {
            session.startTransaction(IsolationMode.READ_COMMITTED);
            try {
                Table table = session.getTable(SimpleDBMContext.TABLE_POST);
                Row searchRow = table.getRow();
                searchRow
                        .setString(SimpleDBMContext.POST_FORUM_NAME, forumName);
                searchRow.setLong(SimpleDBMContext.POST_TOPIC_ID, topicId);
                TableScan scan = table.openScan(0, searchRow, false);
                try {
                    Row row = scan.fetchNext();
                    while (row != null) {
                        Post post = new Post();
                        post.setForumName(row.getString(SimpleDBMContext.POST_FORUM_NAME));
                        post.setTopicId(row.getLong(SimpleDBMContext.POST_TOPIC_ID));
                        post.setPostId(row.getLong(SimpleDBMContext.POST_POST_ID));
                        post.setAuthor(row.getString(SimpleDBMContext.POST_AUTHOR));
                        post.setContent(row.getString(SimpleDBMContext.POST_CONTENT));
                        //                        post.setDateTime(row.getDate(SimpleDBMContext.POST_DATE_TIME));
                        posts.add(post);
                        row = scan.fetchNext();
                    }
                } finally {
                    scan.close();
                }
            } finally {
                session.commit();
            }
        } finally {
            session.close();
        }
        return posts.toArray(new Post[0]);
    }

    public void savePost(Post post) {
        SequenceGenerator seq = new SequenceGenerator(sdbmContext,
                "post_sequence");
        long postId = seq.getNextSequence();
        SessionManager sm = sdbmContext.getSessionManager();
        Session session = sm.openSession();
        try {
            session.startTransaction(IsolationMode.READ_COMMITTED);
            boolean success = false;
            try {
                Table table = session.getTable(SimpleDBMContext.TABLE_POST);
                Row row = table.getRow();
                row.setString(SimpleDBMContext.POST_FORUM_NAME, "");
                row.setLong(SimpleDBMContext.POST_TOPIC_ID, post.getTopicId());
                row.setLong(SimpleDBMContext.POST_POST_ID, postId);
                row.setString(SimpleDBMContext.POST_AUTHOR, post.getAuthor());
                //                row.setDate(SimpleDBMContext.POST_DATE_TIME, post.getDateTime());
                row.setString(SimpleDBMContext.POST_CONTENT, post.getContent());
                table.addRow(row);
            } finally {
                if (success)
                    session.commit();
                else
                    session.rollback();
            }
        } finally {
            session.close();
        }
    }

    public void saveTopic(Topic topic, Post post) {
        SequenceGenerator seq = new SequenceGenerator(sdbmContext,
                "topic_sequence");
        long topicId = seq.getNextSequence();
        SessionManager sm = sdbmContext.getSessionManager();
        Session session = sm.openSession();
        try {
            session.startTransaction(IsolationMode.READ_COMMITTED);
            boolean success = false;
            try {
                Table table = session.getTable(SimpleDBMContext.TABLE_TOPIC);
                Row row = table.getRow();
                row.setString(SimpleDBMContext.TOPIC_FORUM_NAME, topic
                        .getForumName());
                row.setString(SimpleDBMContext.TOPIC_TITLE, topic.getTitle());
                row.setLong(SimpleDBMContext.TOPIC_TOPIC_ID, topicId);
                row.setString(SimpleDBMContext.TOPIC_STARTED_BY, post
                        .getAuthor());
                //                row.setString(SimpleDBMContext.TOPIC_LAST_UPDATED_BY, post.getAuthor());
                //                row.setDate(SimpleDBMContext.TOPIC_LAST_UPDATED_ON, post.getDateTime());
                //                row.setString(SimpleDBMContext.TO, post.getContent());
                table.addRow(row);
            } finally {
                if (success)
                    session.commit();
                else
                    session.rollback();
            }
        } finally {
            session.close();
        }
        savePost(post);
    }

}
