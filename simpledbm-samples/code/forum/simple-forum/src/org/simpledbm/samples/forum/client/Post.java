package org.simpledbm.samples.forum.client;

import java.io.Serializable;

public class Post implements Serializable {
    private static final long serialVersionUID = 1L;
    String author;
    String dateTime;
    String content;

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
