package org.simpledbm.samples.forum.client;

import java.io.Serializable;

/**
 * A Post represents a single posting by a user.
 * Posts have to be against a topic.
 * @author dibyendumajumdar
 */
@SuppressWarnings("serial")
public class Post implements Serializable {
    String author = "anonymous";
    String dateTime = "10:50";
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

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
