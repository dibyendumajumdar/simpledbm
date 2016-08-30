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

import com.google.gwt.dom.client.Style.Visibility;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.ui.Anchor;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.FlowPanel;
import com.google.gwt.user.client.ui.Widget;

public class TopicsMenu extends Composite implements ClickHandler {

    FlowPanel panel = new FlowPanel();
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
        newTopicButton.setStyleName("pager");
        panel.add(newTopicButton);
        panel.add(olderButton);
        panel.add(newerButton);
        newTopicButton.addClickHandler(this);
        newerButton.addClickHandler(this);
        olderButton.addClickHandler(this);
        initWidget(panel);
    }

    public void update(int startIndex, int count, int max) {
        setVisibility(newerButton, startIndex != 0);
        setVisibility(olderButton, startIndex
                + TopicsViewImpl.VISIBLE_TOPICS_COUNT < count);
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
            onNewerClicked(event);
        } else if (sender == newTopicButton) {
            topicsHandler.onNewTopic();
        }
    }
}
