package org.simpledbm.samples.forum.client;

import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.layout.client.Layout.Alignment;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.LayoutPanel;

public class TopPanel extends Composite implements ClickHandler {

    private HTML signOutLink = new HTML("<a href='javascript:;'>Sign Out</a>");
    private HTML aboutLink = new HTML("<a href='javascript:;'>About</a>");

    public TopPanel() {
        LayoutPanel panel = new LayoutPanel();
        HorizontalPanel inner = new HorizontalPanel();
        inner.setSpacing(10);
        inner.add(aboutLink);
        inner.add(signOutLink);
        panel.add(inner);
        panel.setWidgetHorizontalPosition(inner, Alignment.END);

        signOutLink.addClickHandler(this);
        aboutLink.addClickHandler(this);

        initWidget(panel);
    }

    public void onClick(ClickEvent event) {
        Object sender = event.getSource();
        if (sender == signOutLink) {
            Window
                    .alert("If this were implemented, you would be signed out now.");
        } else if (sender == aboutLink) {
            // When the 'About' item is selected, show the AboutDialog.
            // Note that showing a dialog box does not block -- execution continues
            // normally, and the dialog fires an event when it is closed.
            AboutDialog dlg = new AboutDialog();
            dlg.show();
            dlg.center();
        }
    }

}
