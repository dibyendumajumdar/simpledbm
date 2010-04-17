/***
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program; if not, write to the Free Software
 *    Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *    
 *    Linking this library statically or dynamically with other modules 
 *    is making a combined work based on this library. Thus, the terms and
 *    conditions of the GNU General Public License cover the whole
 *    combination.
 *    
 *    As a special exception, the copyright holders of this library give 
 *    you permission to link this library with independent modules to 
 *    produce an executable, regardless of the license terms of these 
 *    independent modules, and to copy and distribute the resulting 
 *    executable under terms of your choice, provided that you also meet, 
 *    for each linked independent module, the terms and conditions of the 
 *    license of that module.  An independent module is a module which 
 *    is not derived from or based on this library.  If you modify this 
 *    library, you may extend this exception to your version of the 
 *    library, but you are not obligated to do so.  If you do not wish 
 *    to do so, delete this exception statement from your version.
 *
 *    Project: www.simpledbm.org
 *    Author : Dibyendu Majumdar
 *    Email  : d dot majumdar at gmail dot com ignore
 */
package org.simpledbm.samples.forum.server;

import java.util.ArrayList;
import java.util.Date;

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
            boolean success = true;
            try {
                Table table = session.getTable(SimpleDBMContext.TABLE_TOPIC);
                Row row = table.getRow();
                row.setString(0, forumName);
                TableScan scan = table.openScan(0, row, false);
                try {
                    row = scan.fetchNext();
                    while (row != null) {
//                        System.err.println("Read row " + row);
                        Topic t = new Topic();
                        t.setForumName(row
                                .getString(SimpleDBMContext.TOPIC_FORUM_NAME));
                        t.setTopicId(row
                                .getLong(SimpleDBMContext.TOPIC_TOPIC_ID));
                        t.setStartedBy(row
                                .getString(SimpleDBMContext.TOPIC_STARTED_BY));
                        t.setTitle(row.getString(SimpleDBMContext.TOPIC_TITLE));
                        if (t.getForumName().equals(forumName)) {
                            topics.add(t);
                        } else {
                            break;
                        }
                        row = scan.fetchNext();
                    }
                } finally {
                    scan.close();
                }
                success = true;
            } finally {
                if (success)
                    session.commit();
                else
                    session.rollback();
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
            boolean success = true;
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
                success = true;
            } finally {
                if (success)
                    session.commit();
                else
                    session.rollback();
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
            boolean success = false;
            try {
                Table table = session.getTable(SimpleDBMContext.TABLE_POST);
                Row searchRow = table.getRow();
                searchRow
                        .setString(SimpleDBMContext.POST_FORUM_NAME, forumName);
                searchRow.setLong(SimpleDBMContext.POST_TOPIC_ID, topicId);
//                System.err.println("Searching for topic " + searchRow);
                TableScan scan = table.openScan(0, searchRow, false);
                try {
                    Row row = scan.fetchNext();
                    while (row != null) {
                        Post post = new Post();
                        post.setForumName(row
                                .getString(SimpleDBMContext.POST_FORUM_NAME));
                        post.setTopicId(row
                                .getLong(SimpleDBMContext.POST_TOPIC_ID));
                        post.setPostId(row
                                .getLong(SimpleDBMContext.POST_POST_ID));
                        post.setAuthor(row
                                .getString(SimpleDBMContext.POST_AUTHOR));
                        post.setContent(row
                                .getString(SimpleDBMContext.POST_CONTENT));
                        post.setDateTime(row
                                .getString(SimpleDBMContext.POST_DATE_TIME));
                        if (post.getForumName().equals(forumName)
                                && post.getTopicId() == topicId) {
//                            System.err.println(row);
                            posts.add(post);
                        } else {
                            break;
                        }
                        row = scan.fetchNext();
                    }
                } finally {
                    scan.close();
                }
                success = true;
            } finally {
                if (success)
                    session.commit();
                else
                    session.rollback();
            }
        } finally {
            session.close();
        }
        return posts.toArray(new Post[0]);
    }

    public void savePost(Post post) {
        if (!FieldVerifier.isValidPost(post)) {
            throw new IllegalArgumentException("Post must have all fields completed");
        }
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
                row.setString(SimpleDBMContext.POST_FORUM_NAME, post
                        .getForumName());
                row.setLong(SimpleDBMContext.POST_TOPIC_ID, post.getTopicId());
                row.setLong(SimpleDBMContext.POST_POST_ID, postId);
                row.setString(SimpleDBMContext.POST_AUTHOR, post.getAuthor());
                row.setDate(SimpleDBMContext.POST_DATE_TIME, new Date());
                row.setString(SimpleDBMContext.POST_CONTENT, post.getContent());
//                System.err.println("Saving post " + row);
                table.addRow(row);
                success = true;
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
        if (!FieldVerifier.isValidTopic(topic)) {
            throw new IllegalArgumentException("Topic must have all fields completed");
        }
        if (!FieldVerifier.isValidPost(post)) {
            throw new IllegalArgumentException("Post must have all fields completed");
        }
        SequenceGenerator seq = new SequenceGenerator(sdbmContext,
                "topic_sequence");
        long topicId = seq.getNextSequence();
        post.setForumName(topic.getForumName());
        post.setTopicId(topicId);
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
//                System.err.println("Saving topic " + row);
                table.addRow(row);
                success = true;
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
