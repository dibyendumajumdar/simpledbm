package org.simpledbm.samples.forum.client;

import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.FlowPanel;
import com.google.gwt.user.client.ui.ScrollPanel;

public class TopicPosts extends Composite {

    ScrollPanel sp = new ScrollPanel();
    FlowPanel panel = new FlowPanel();
    private FlexTable header = new FlexTable();

    public TopicPosts() {
        panel.setStyleName("posts");
        update();
        sp.add(panel);
        initWidget(sp);
    }

    public void update() {
        header.setCellPadding(0);
        header.setCellSpacing(0);
        header.setStyleName("header");
        header.setWidget(0, 0, new PostMenu());
        panel.add(header);
        for (int i = 0; i < 10; i++) {
            panel.add(new Post().toWidget());
        }
        System.err.println(panel);
    }

}
