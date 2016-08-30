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

import com.google.gwt.core.client.GWT;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.rpc.AsyncCallback;

/**
 * RequestProcessor handles the interactions between the Views and the backend
 * services.
 * 
 * @author dibyendumajumdar
 */
public class RequestProcessor implements ForumsHandler, TopicsHandler,
        PostsHandler {

    Forum currentForum;
    Topic currentTopic;

    interface TopicsView {
        void update(Topic[] topicList);

        void showBusy();

        void setTopicsHandler(TopicsHandler topicsHandler);
    }

    interface ForumsView {
        /**
         * Updates the forums on the view
         */
        void update(Forum[] forums);

        void showBusy();

        void setForumsHandler(ForumsHandler forumSelectHandler);
    }

    interface PostsView {

        void update(Post[] posts);

        void showBusy();

        void setPostsHandler(PostsHandler postsHandler);
    }

    /**
     * Create a remote service proxy to talk to the server-side Greeting
     * service.
     */
    private final SimpleForumServiceAsync simpleForumService = GWT
            .create(SimpleForumService.class);

    TopicsView topicsView;
    ForumsView forumsView;
    PostsView postsView;

    public RequestProcessor(TopicsView topicsView, ForumsView forumsView,
            PostsView postsView) {
        this.topicsView = topicsView;
        topicsView.setTopicsHandler(this);
        this.forumsView = forumsView;
        forumsView.setForumsHandler(this);
        this.postsView = postsView;
        postsView.setPostsHandler(this);
    }

    private void getTopics(final String forumName) {
        topicsView.showBusy();
        simpleForumService.getTopics(forumName, new AsyncCallback<Topic[]>() {
            public void onFailure(Throwable caught) {
                Window.alert("Failed to get topics: " + caught.getMessage());
            }

            public void onSuccess(Topic[] topics) {
                topicsView.update(topics);
                if (topics.length > 0) {
                    if (currentTopic == null) {
                        currentTopic = topics[0];
                    } else if (!currentTopic.getForumName().equals(forumName)) {
                        currentTopic = topics[0];
                    }
                    //                    System.err.println("Executing getPosts for forum " + forumName);
                    getPosts(currentTopic.getForumName(), currentTopic
                            .getTopicId());
                } else {
                    postsView.update(new Post[0]);
                }
            }
        });
    }

    public void getForums() {
        forumsView.showBusy();
        simpleForumService.getForums(new AsyncCallback<Forum[]>() {
            public void onFailure(Throwable caught) {
                Window.alert("Failed to get forums: " + caught.getMessage());
            }

            public void onSuccess(Forum[] forums) {
                forumsView.update(forums);
                if (forums != null && forums.length > 0) {
                    currentForum = forums[0];
                    getTopics(currentForum.getName());
                }
            }
        });
    }

    private void getPosts(String forumName, long topicId) {
        postsView.showBusy();
        simpleForumService.getPosts(forumName, topicId,
                new AsyncCallback<Post[]>() {
                    public void onFailure(Throwable caught) {
                        Window.alert("Failed to get posts: "
                                + caught.getMessage());
                    }

                    public void onSuccess(Post[] posts) {
                        postsView.update(posts);
                    }
                });
    }

    public void onForumSelect(String forumName) {
        currentForum = new Forum();
        currentForum.name = forumName;
        currentForum.description = "";
        getTopics(forumName);
    }

    public void onTopicSelection(Topic topic) {
        currentTopic = topic;
        getPosts(topic.getForumName(), topic.getTopicId());
    }

    public void onNewPost() {
        new NewPostViewImpl(currentTopic, this).show();
    }

    public void savePost(final Post post) {
        simpleForumService.savePost(post, new AsyncCallback<Void>() {
            public void onFailure(Throwable caught) {
                Window.alert("Failed to add post: " + post
                        + caught.getMessage());
            }

            public void onSuccess(Void discard) {
                getPosts(post.getForumName(), post.getTopicId());
            }
        });
    }

    public void saveTopic(final Topic topic, final Post post) {
        simpleForumService.saveTopic(topic, post, new AsyncCallback<Void>() {
            public void onFailure(Throwable caught) {
                Window.alert("Failed to add topic: " + topic
                        + caught.getMessage());
            }

            public void onSuccess(Void discard) {
                currentTopic = topic;
                getTopics(topic.getForumName());
            }
        });
    }

    public void onNewTopic() {
        new NewTopicViewImpl(currentForum, this).show();
    }

}
