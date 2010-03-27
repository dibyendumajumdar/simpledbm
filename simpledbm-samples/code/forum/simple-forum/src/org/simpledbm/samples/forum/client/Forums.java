package org.simpledbm.samples.forum.client;

import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.VerticalPanel;

public class Forums extends Composite {
    
    VerticalPanel panel = new VerticalPanel();
    
    private class Forum {
        String name;
        String description;
        public Forum(String name, String description) {
            super();
            this.name = name;
            this.description = description;
        }
        public String getName() {
            return name;
        }
        public String getDescription() {
            return description;
        }
        
    }
    
    public Forums() {
        
        // panel.setBorderWidth(1);
        panel.setSpacing(5);
        panel.setWidth("100%");
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
          }
        });
      }

    
}
