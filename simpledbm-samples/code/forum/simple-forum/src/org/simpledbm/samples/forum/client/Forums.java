package org.simpledbm.samples.forum.client;

import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.FlowPanel;
import com.google.gwt.user.client.ui.HTML;

public class Forums extends Composite {

    FlowPanel panel = new FlowPanel();

    public Forums() {

        panel.setStyleName("forums");
        panel.add(new HTML("<h2>Forums</h2>"));
        addForum(new Forum("Nikon", "Discuss Nikon cameras"));
        addForum(new Forum("Leica", "Discuss Leica cameras"));

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
                System.err.println("Clicked " + link.getText());
            }
        });
    }
}
