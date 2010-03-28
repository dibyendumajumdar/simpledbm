package org.simpledbm.samples.forum.client;

import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.dom.client.KeyCodes;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.DialogBox;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.VerticalPanel;

public class AboutDialog extends DialogBox {
    public AboutDialog() {
        // Use this opportunity to set the dialog's caption.
        setText("About the SimpleForum Sample");

        // Create a VerticalPanel to contain the 'about' label and the 'OK' button.
        VerticalPanel outer = new VerticalPanel();

        // Create the 'about' text and set a style name so we can style it with CSS.

        HTML text = new HTML(
                "This sample application demonstrates the "
                        + "SimpleDBM's network client server API in a simple "
                        + "web application. The user interface of this application is "
                        + "built using GWT.");
        text.setStyleName("mail-AboutText");
        outer.add(text);

        // Create the 'OK' button, along with a handler that hides the dialog
        // when the button is clicked.
        outer.add(new Button("Close", new ClickHandler() {
            public void onClick(ClickEvent event) {
                hide();
            }
        }));

        setWidget(outer);
    }

    @Override
    public boolean onKeyDownPreview(char key, int modifiers) {
        // Use the popup's key preview hooks to close the dialog when either
        // enter or escape is pressed.
        switch (key) {
        case KeyCodes.KEY_ENTER:
        case KeyCodes.KEY_ESCAPE:
            hide();
            break;
        }

        return true;
    }

}
