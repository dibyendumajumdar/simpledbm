package org.simpledbm.samples.forum.client;

import org.simpledbm.samples.forum.client.RequestProcessor.ForumsHandler;

import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.FlowPanel;
import com.google.gwt.user.client.ui.HTML;

public class Forums extends Composite implements ForumsHandler {

    FlowPanel panel = new FlowPanel();
    final Topics topics;

    public Forums(Topics topics) {
        this.topics = topics;
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
                //                topics.getTopics(link.getText());
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
}
