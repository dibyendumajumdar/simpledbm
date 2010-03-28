package org.simpledbm.samples.forum.client;

import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.FlowPanel;
import com.google.gwt.user.client.ui.ScrollPanel;

public class Posts extends Composite {

    ScrollPanel sp = new ScrollPanel();
    FlowPanel panel = new FlowPanel();
    private FlexTable header = new FlexTable();

    public Posts() {
        panel.setStyleName("posts");
        update();
        sp.add(panel);
        initWidget(sp);
    }

    public void update() {
        header.setCellPadding(0);
        header.setCellSpacing(0);
        header.setStyleName("header");
        header.setWidget(0, 0, new PostsMenu());
        panel.add(header);
        for (int i = 0; i < 10; i++) {
            addPost(new Post());
        }
        System.err.println(panel);
    }
    
    void addPost(Post post) {
        FlexTable table = new FlexTable();
        table.setCellPadding(0);
        table.setCellSpacing(0);
        table.setStyleName("post");
        table.setText(0, 0, "author: " + post.getAuthor());
        table.getRowFormatter().setStyleName(0, "postheader");
        table.setText(1, 0, "this is the body of the post");
        System.err.println(table);
        panel.add(table);
    }

}
