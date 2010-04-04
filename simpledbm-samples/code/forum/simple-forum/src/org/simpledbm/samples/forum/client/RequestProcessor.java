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

    private void getTopics(String forumName) {
        topicsView.showBusy();
        simpleForumService.getTopics(forumName, new AsyncCallback<Topic[]>() {
            public void onFailure(Throwable caught) {
                // Show the RPC error message to the user
                Window.alert("Failed to get topics: " + caught.getMessage());
            }

            public void onSuccess(Topic[] topicList) {
                topicsView.update(topicList);
                if (topicList.length > 0) {
                    getPosts(topicList[0].getTitle());
                }
            }
        });
    }

    public void getForums() {
        forumsView.showBusy();
        simpleForumService.getForums(new AsyncCallback<Forum[]>() {
            public void onFailure(Throwable caught) {
                // Show the RPC error message to the user
                Window.alert("Failed to get forums: " + caught.getMessage());
            }

            public void onSuccess(Forum[] forums) {
                forumsView.update(forums);
                if (forums != null && forums.length > 0) {
                    getTopics(forums[0].getName());
                }
            }
        });
    }

    private void getPosts(String topicId) {
        postsView.showBusy();
        simpleForumService.getPosts(topicId, new AsyncCallback<Post[]>() {
            public void onFailure(Throwable caught) {
                // Show the RPC error message to the user
                Window.alert("Failed to get posts: " + caught.getMessage());
            }

            public void onSuccess(Post[] posts) {
                postsView.update(posts);
            }
        });
    }

    public void onForumSelect(String forumName) {
        getTopics(forumName);
    }

    public void onTopicSelection(String topicId) {
        getPosts(topicId);
    }

    public void onNewPost() {
        new NewPostViewImpl(this).show();
    }

    public void savePost(Post post) {
        simpleForumService.savePost(post, new AsyncCallback<Void>() {
            public void onFailure(Throwable caught) {
                // Show the RPC error message to the user
                Window.alert("Failed to add post: " + caught.getMessage());
            }

            public void onSuccess(Void discard) {
            }
        });
    }

    public void saveTopic(Topic topic, Post post) {
        simpleForumService.saveTopic(topic, post, new AsyncCallback<Void>() {
            public void onFailure(Throwable caught) {
                // Show the RPC error message to the user
                Window.alert("Failed to add topic: " + caught.getMessage());
            }

            public void onSuccess(Void discard) {
            }
        });
    }

    public void onNewTopic() {
        new NewTopicViewImpl(this).show();
    }

}
