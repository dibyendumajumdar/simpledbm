package org.simpledbm.samples.forum.client;

import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.ui.Anchor;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.FlowPanel;

public class PostsMenu extends Composite implements ClickHandler {

    FlowPanel panel = new FlowPanel();
    private Anchor addButton = new Anchor("New post");

    public PostsMenu() {
        addButton.setStyleName("pager");
        panel.add(addButton);
        initWidget(panel);
    }

    public void onClick(ClickEvent event) {
        Object sender = event.getSource();
        if (sender == addButton) {
            // TODO Add a post
            // Open a dialog
        }
    }
}
