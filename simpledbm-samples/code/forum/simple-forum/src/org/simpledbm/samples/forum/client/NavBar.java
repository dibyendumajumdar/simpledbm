package org.simpledbm.samples.forum.client;

import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.LayoutPanel;

public class NavBar extends Composite {

    LayoutPanel panel = new LayoutPanel();
    private HTML countLabel = new HTML();
    private HTML newerButton = new HTML(
            "<a href='javascript:;'>&lt; newer</a>", true);
    private HTML olderButton = new HTML(
            "<a href='javascript:;'>older &gt;</a>", true);

    private final ForumTopics outer;

    public NavBar(ForumTopics outer) {
        panel.add(newerButton);
        panel.add(countLabel);
        panel.add(olderButton);
        initWidget(panel);
        this.outer = outer;
    }

    public void update(int startIndex, int count, int max) {
        newerButton.setVisible(startIndex != 0);
        olderButton
                .setVisible(startIndex + ForumTopics.VISIBLE_TOPICS_COUNT < count);
        countLabel
                .setText("" + (startIndex + 1) + " - " + max + " of " + count);
    }

    void onNewerClicked(ClickEvent event) {
        outer.newer();
    }

    void onOlderClicked(ClickEvent event) {
        outer.older();
    }

}
