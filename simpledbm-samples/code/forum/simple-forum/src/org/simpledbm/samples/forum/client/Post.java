package org.simpledbm.samples.forum.client;

import java.io.Serializable;
import java.util.Date;

import com.google.gwt.i18n.client.DateTimeFormat;

/**
 * A Post represents a single posting by a user. Posts have to be against a
 * topic.
 * 
 * @author dibyendumajumdar
 */
@SuppressWarnings("serial")
public class Post implements Serializable {
    String forumName;
    long topicId;
    long postId;
    String author = "anonymous";
    String dateTime;
    String content = "content not set";

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(Date d) {
        dateTime = DateTimeFormat.getShortDateTimeFormat().format(d);
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public long getTopicId() {
        return topicId;
    }

    public void setTopicId(long topicId) {
        this.topicId = topicId;
    }

    public String getForumName() {
        return forumName;
    }

    public void setForumName(String forumName) {
        this.forumName = forumName;
    }

    public long getPostId() {
        return postId;
    }

    public void setPostId(long postId) {
        this.postId = postId;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    @Override
    public String toString() {
        return "Post [author=" + author + ", content=" + content
                + ", dateTime=" + dateTime + ", topicId=" + topicId + "]";
    }

}
