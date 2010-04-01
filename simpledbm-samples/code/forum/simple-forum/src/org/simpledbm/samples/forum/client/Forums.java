package org.simpledbm.samples.forum.client;

import org.simpledbm.samples.forum.client.RequestProcessor.ForumsViewHandler;

import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.FlowPanel;
import com.google.gwt.user.client.ui.HTML;

public class Forums extends Composite implements ForumsViewHandler {

    FlowPanel panel = new FlowPanel();
    private ForumsHandler forumSelectionHandler;

    public Forums(Topics topics) {
        initWidget(panel);
    }

    private void addForum(final Forum forum) {
        final HTML link = new HTML("<a href='javascript:;'>" + forum.getName()
                + "</a>");
        link.setTitle(forum.getDescription());
        panel.add(link);

        // Add a click handler that displays a ContactPopup when it is clicked.
        link.addClickHandler(new ClickHandler() {
            public void onClick(ClickEvent event) {
                forumSelectionHandler.onForumSelect(link.getText());
            }
        });
    }

    public void showBusy() {
    }

    public void update(Forum[] forums) {
        panel.clear();
        panel.setStyleName("forums");
        panel.add(new HTML("<h2>Forums</h2>"));
        for (int i = 0; i < forums.length; i++) {
            addForum(forums[i]);
        }
    }

    public void setForumsHandler(ForumsHandler forumSelectHandler) {
        this.forumSelectionHandler = forumSelectHandler;
    }
}
