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

import org.simpledbm.samples.forum.client.RequestProcessor.PostsView;

import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.FlowPanel;
import com.google.gwt.user.client.ui.ScrollPanel;

public class PostsViewImpl extends Composite implements PostsView {

    ScrollPanel sp = new ScrollPanel();
    FlowPanel panel = new FlowPanel();
    private FlexTable header = new FlexTable();
    private PostsHandler postsHandler;
    
    public PostsHandler getPostsHandler() {
        return postsHandler;
    }

    public PostsViewImpl() {
        initHeader();
        sp.add(panel);
        sp.setStyleName("posts");
        initWidget(sp);
    }

    public void initHeader() {
        header.setCellPadding(0);
        header.setCellSpacing(0);
        header.setStyleName("postMenubar");
        header.setWidget(0, 0, new PostsMenu(postsHandler));
        panel.add(header);
    }
    
    void addPost(Post post) {
        FlexTable table = new FlexTable();
        table.setCellPadding(0);
        table.setCellSpacing(0);
        table.setStyleName("post");
        table.setText(0, 0, "author: " + post.getAuthor());
        table.getRowFormatter().setStyleName(0, "postheader");
        table.setText(1, 0, post.getContent());
        panel.add(table);
    }

    public void setPostsHandler(PostsHandler postsHandler) {
        this.postsHandler = postsHandler;
    }

    public void showBusy() {
    }

    public void update(Post[] posts) {
        panel.clear();
        initHeader();
        for (int i = 0; i < posts.length; i++) {
            addPost(posts[i]);
        }
    }

}
