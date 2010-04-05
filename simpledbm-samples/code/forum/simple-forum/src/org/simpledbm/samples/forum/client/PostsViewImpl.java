package org.simpledbm.samples.forum.client;

import org.simpledbm.samples.forum.client.RequestProcessor.PostsView;

import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.FlowPanel;
import com.google.gwt.user.client.ui.ScrollPanel;

public class PostsViewImpl extends Composite implements PostsView {

    ScrollPanel sp = new ScrollPanel();
    FlowPanel panel = new FlowPanel();
    private FlexTable header = new FlexTable();
    private PostsHandler postsHandler;
    
    public PostsHandler getPostsHandler() {
        return postsHandler;
    }

    public PostsViewImpl() {
        panel.setStyleName("posts");
        initHeader();
        sp.add(panel);
        initWidget(sp);
    }

    public void initHeader() {
        header.setCellPadding(0);
        header.setCellSpacing(0);
        header.setStyleName("header");
        header.setWidget(0, 0, new PostsMenu(postsHandler));
        panel.add(header);
    }
    
    void addPost(Post post) {
        FlexTable table = new FlexTable();
        table.setCellPadding(0);
        table.setCellSpacing(0);
        table.setStyleName("post");
        table.setText(0, 0, "author: " + post.getAuthor());
        table.getRowFormatter().setStyleName(0, "postheader");
        table.setText(1, 0, post.getContent());
        panel.add(table);
    }

    public void setPostsHandler(PostsHandler postsHandler) {
        this.postsHandler = postsHandler;
    }

    public void showBusy() {
    }

    public void update(Post[] posts) {
        panel.clear();
        initHeader();
        for (int i = 1; i < posts.length; i++) {
            addPost(posts[i]);
        }
    }

}
