/***
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program; if not, write to the Free Software
 *    Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *    
 *    Linking this library statically or dynamically with other modules 
 *    is making a combined work based on this library. Thus, the terms and
 *    conditions of the GNU General Public License cover the whole
 *    combination.
 *    
 *    As a special exception, the copyright holders of this library give 
 *    you permission to link this library with independent modules to 
 *    produce an executable, regardless of the license terms of these 
 *    independent modules, and to copy and distribute the resulting 
 *    executable under terms of your choice, provided that you also meet, 
 *    for each linked independent module, the terms and conditions of the 
 *    license of that module.  An independent module is a module which 
 *    is not derived from or based on this library.  If you modify this 
 *    library, you may extend this exception to your version of the 
 *    library, but you are not obligated to do so.  If you do not wish 
 *    to do so, delete this exception statement from your version.
 *
 *    Project: www.simpledbm.org
 *    Author : Dibyendu Majumdar
 *    Email  : d dot majumdar at gmail dot com ignore
 */
package org.simpledbm.samples.forum.client;

import java.util.Date;

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
    private final Forum forum;
    VerticalPanel outer = new VerticalPanel();
    TextBox topicText = new TextBox();
    TextBox author = new TextBox();
    RichTextArea textArea = new RichTextArea();
    Button saveButton = new Button("Save");
    Button cancelButton = new Button("Cancel");

    public NewTopicViewImpl(Forum forum, TopicsHandler postsHandler) {
        this.forum = forum;
        this.topicsHandler = postsHandler;
        setText("Add a new Topic");
        outer.add(new Label("Topic:"));
        outer.add(topicText);
        outer.add(new Label("Your name:"));
        outer.add(author);
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
            post.setAuthor(author.getText());
            post.setDateTime(new Date());
            Topic topic = new Topic();
            topic.setForumName(forum.getName());
            topic.setTitle(topicText.getText());
            topic.setStartedBy(post.getAuthor());
            topicsHandler.saveTopic(topic, post);
        } else {
        }
        hide();
    }

}
