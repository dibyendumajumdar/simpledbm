package org.simpledbm.samples.forum.client;

import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.Widget;

public class Post {

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

    Widget toWidget() {
        FlexTable table = new FlexTable();
        table.setCellPadding(0);
        table.setCellSpacing(0);
        table.setStyleName("post");
        table.setText(0, 0, "author: " + author);
        table.getRowFormatter().setStyleName(0, "postheader");
        table.setText(1, 0, "this is the body of the post");
        System.err.println(table);
        return table;
    }
}
