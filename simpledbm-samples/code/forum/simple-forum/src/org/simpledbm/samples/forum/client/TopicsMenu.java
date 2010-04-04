package org.simpledbm.samples.forum.client;

import com.google.gwt.dom.client.Style.Visibility;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.ui.Anchor;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.FlowPanel;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.Widget;

public class TopicsMenu extends Composite implements ClickHandler {

    FlowPanel panel = new FlowPanel();
    private Label countLabel = new Label();
    private Anchor newerButton = new Anchor("< newer");
    private Anchor olderButton = new Anchor("older >");

    private final TopicsViewImpl outer;

    public TopicsMenu(TopicsViewImpl outer) {
        newerButton.setStyleName("pager");
        olderButton.setStyleName("pager");
        countLabel.setStyleName("pager");
        panel.add(newerButton);
        panel.add(countLabel);
        panel.add(olderButton);
        initWidget(panel);
        this.outer = outer;
    }

    public void update(int startIndex, int count, int max) {
        setVisibility(newerButton, startIndex != 0);
        setVisibility(olderButton, startIndex
                + TopicsViewImpl.VISIBLE_TOPICS_COUNT < count);
        countLabel
                .setText("" + (startIndex + 1) + " - " + max + " of " + count);
        System.err.println(panel);
    }

    void onNewerClicked(ClickEvent event) {
        outer.newer();
    }

    void onOlderClicked(ClickEvent event) {
        outer.older();
    }

    private void setVisibility(Widget widget, boolean visible) {
        widget.getElement().getStyle().setVisibility(
                visible ? Visibility.VISIBLE : Visibility.VISIBLE);
    }

    public void onClick(ClickEvent event) {
        Object sender = event.getSource();
        if (sender == olderButton) {
            onOlderClicked(event);
        } else if (sender == newerButton) {
            // Move back a page.
            onNewerClicked(event);
        }
    }
}
