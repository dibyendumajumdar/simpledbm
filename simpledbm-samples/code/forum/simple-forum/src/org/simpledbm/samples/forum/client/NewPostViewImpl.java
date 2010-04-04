package org.simpledbm.samples.forum.client;

import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.dom.client.KeyCodes;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.DialogBox;
import com.google.gwt.user.client.ui.FlowPanel;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.RichTextArea;
import com.google.gwt.user.client.ui.VerticalPanel;

public class NewPostViewImpl extends DialogBox implements ClickHandler {
    
    private final PostsHandler postsHandler;
    VerticalPanel outer = new VerticalPanel();
    RichTextArea textArea = new RichTextArea();
    Button saveButton = new Button("Save");
    Button cancelButton = new Button("Cancel");
    
    public NewPostViewImpl(PostsHandler postsHandler) {
        this.postsHandler = postsHandler;
        setText("Add a new post");
        outer.add(new Label("Enter post:"));
        outer.add(textArea);
        saveButton.addClickHandler(this);
        cancelButton.addClickHandler(this);
        FlowPanel panel = new FlowPanel();
        panel.add(saveButton);
        panel.add(cancelButton);
        outer.add(panel);
        setWidget(outer);
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
            postsHandler.savePost(post);
        }
        else {
        }
        hide();
    }

}
