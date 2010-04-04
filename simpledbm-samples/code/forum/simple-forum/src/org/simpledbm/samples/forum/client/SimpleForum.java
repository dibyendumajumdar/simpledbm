package org.simpledbm.samples.forum.client;

import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.dom.client.Style.Unit;
import com.google.gwt.user.client.ui.DockLayoutPanel;
import com.google.gwt.user.client.ui.RootLayoutPanel;
import com.google.gwt.user.client.ui.SplitLayoutPanel;

/**
 * Entry point classes define <code>onModuleLoad()</code>.
 */
public class SimpleForum implements EntryPoint {
    
    TopPanel topPanel = new TopPanel();
    Topics topics = new Topics();
    Forums forums = new Forums();
    Posts posts = new Posts();

    
    RequestProcessor requestProcessor = new RequestProcessor(topics, forums, posts);
    
    /**
     * This is the entry point method.
     */
    public void onModuleLoad() {
        DockLayoutPanel outer = new DockLayoutPanel(Unit.EM);
        outer.addNorth(topPanel, 5);
        SplitLayoutPanel p = new SplitLayoutPanel();
        p.addWest(forums, 192);
        p.addNorth(topics, 200);
        p.add(posts);
        outer.add(p);
        RootLayoutPanel root = RootLayoutPanel.get();
        root.add(outer);
        requestProcessor.getForums();
    }
}
