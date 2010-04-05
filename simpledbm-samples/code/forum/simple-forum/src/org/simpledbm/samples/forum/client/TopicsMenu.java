package org.simpledbm.samples.forum.client;

import com.google.gwt.dom.client.Style.Visibility;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.ui.Anchor;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.FlowPanel;
import com.google.gwt.user.client.ui.Widget;

public class TopicsMenu extends Composite implements ClickHandler {

    FlowPanel panel = new FlowPanel();
//    private Label countLabel = new Label();
    private Anchor newerButton = new Anchor("< newer");
    private Anchor olderButton = new Anchor("older >");
    private Anchor newTopicButton = new Anchor("New Topic");

    private final TopicsViewImpl outer;
    private TopicsHandler topicsHandler;

    public void setTopicsHandler(TopicsHandler topicsHandler) {
        this.topicsHandler = topicsHandler;
    }

    public TopicsHandler getTopicsHandler() {
        return topicsHandler;
    }

    public TopicsMenu(TopicsViewImpl outer) {
        this.outer = outer;
        newerButton.setStyleName("pager");
        olderButton.setStyleName("pager");
//        countLabel.setStyleName("pager");
        newTopicButton.setStyleName("pager");
        panel.add(newTopicButton);
        panel.add(olderButton);
        panel.add(newerButton);
        newTopicButton.addClickHandler(this);
        newerButton.addClickHandler(this);
        olderButton.addClickHandler(this);
//        panel.add(countLabel);
        initWidget(panel);
    }

    public void update(int startIndex, int count, int max) {
        setVisibility(newerButton, startIndex != 0);
        setVisibility(olderButton, startIndex
                + TopicsViewImpl.VISIBLE_TOPICS_COUNT < count);
//        countLabel
//                .setText("" + (startIndex + 1) + " - " + max + " of " + count);
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
        } else if (sender == newTopicButton) {
            topicsHandler.onNewTopic();
        }
    }
}
