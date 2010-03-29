package org.simpledbm.samples.forum.client;

import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.dom.client.KeyCodes;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.DialogBox;
import com.google.gwt.user.client.ui.FlowPanel;
import com.google.gwt.user.client.ui.HTML;

public class AboutDialog extends DialogBox {
    public AboutDialog() {
        setText("About the SimpleForum Sample");
        FlowPanel panel = new FlowPanel();
        HTML text = new HTML("This sample application demonstrates the "
                + "SimpleDBM's network client server API in a simple "
                + "web application. The user interface of this application is "
                + "built using GWT.");
        //        text.setStyleName("mail-AboutText");
        panel.add(text);
        panel.add(new Button("Done", new ClickHandler() {
            public void onClick(ClickEvent event) {
                hide();
            }
        }));
        setWidget(panel);
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

}
