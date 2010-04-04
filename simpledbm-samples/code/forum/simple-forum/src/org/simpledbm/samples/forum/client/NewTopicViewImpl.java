package org.simpledbm.samples.forum.client;

import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.dom.client.KeyCodes;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.DialogBox;
import com.google.gwt.user.client.ui.FlowPanel;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.RichTextArea;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.VerticalPanel;

public class NewTopicViewImpl extends DialogBox implements ClickHandler {
    
    private final TopicsHandler topicsHandler;
    VerticalPanel outer = new VerticalPanel();
    TextBox topicText = new TextBox();
    RichTextArea textArea = new RichTextArea();
    Button saveButton = new Button("Save");
    Button cancelButton = new Button("Cancel");
    
    public NewTopicViewImpl(TopicsHandler postsHandler) {
        this.topicsHandler = postsHandler;
        setText("Add a new Topic");
        outer.add(new Label("Topic:"));
        outer.add(topicText);
        outer.add(new Label("Initial Post:"));
        outer.add(textArea);
        FlowPanel buttons = new FlowPanel();
        buttons.add(saveButton);
        buttons.add(cancelButton);
        outer.add(buttons);
        setWidget(outer);
        saveButton.addClickHandler(this);
        cancelButton.addClickHandler(this);
    }

    @Override
    public boolean onKeyDownPreview(char key, int modifiers) {
        switch (key) {
        case KeyCodes.KEY_ENTER:
        case KeyCodes.KEY_ESCAPE:
            hide();
            break;
        }
        return true;
    }

    public void onClick(ClickEvent event) {
        Object source = event.getSource();
        if (source == saveButton) {
            Post post = new Post();
            post.setContent(textArea.getText());
            Topic topic = new Topic();
            topic.setTitle(topicText.getText());
            topicsHandler.saveTopic(topic, post);
        }
        else {
        }
        hide();
    }

}
