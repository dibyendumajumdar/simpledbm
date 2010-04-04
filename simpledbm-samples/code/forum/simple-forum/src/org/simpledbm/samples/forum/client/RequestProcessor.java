package org.simpledbm.samples.forum.client;

import com.google.gwt.core.client.GWT;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.rpc.AsyncCallback;

public class RequestProcessor implements ForumsHandler, TopicsHandler,
        PostsHandler {

    interface TopicsViewHandler {
        void update(TopicList topicList);

        void showBusy();

        void setTopicsHandler(TopicsHandler topicsHandler);
    }

    interface ForumsViewHandler {
        /**
         * Updates the forums on the view
         */
        void update(Forum[] forums);

        void showBusy();

        void setForumsHandler(ForumsHandler forumSelectHandler);
    }

    interface PostsViewHandler {

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

    TopicsViewHandler topicsViewHandler;
    ForumsViewHandler forumsViewHandler;
    PostsViewHandler postsViewHandler;

    public RequestProcessor(TopicsViewHandler topicListHandler,
            ForumsViewHandler forumsHandler, PostsViewHandler postsViewHandler) {
        this.topicsViewHandler = topicListHandler;
        topicListHandler.setTopicsHandler(this);
        this.forumsViewHandler = forumsHandler;
        forumsHandler.setForumsHandler(this);
        this.postsViewHandler = postsViewHandler;
        postsViewHandler.setPostsHandler(this);
    }

    void getTopics(String forumName) {
        topicsViewHandler.showBusy();
        simpleForumService.getTopics(forumName, new AsyncCallback<TopicList>() {
            public void onFailure(Throwable caught) {
                // Show the RPC error message to the user
                Window.alert("Failed to get topics: " + caught.getMessage());
            }

            public void onSuccess(TopicList topicList) {
                topicsViewHandler.update(topicList);
                if (topicList.getTopicCount() > 0) {
                    getPosts(topicList.getTopic(0).getTitle());
                }
            }
        });
    }

    void getForums() {
        forumsViewHandler.showBusy();
        simpleForumService.getForums(new AsyncCallback<Forum[]>() {
            public void onFailure(Throwable caught) {
                // Show the RPC error message to the user
                Window.alert("Failed to get forums: " + caught.getMessage());
            }

            public void onSuccess(Forum[] forums) {
                forumsViewHandler.update(forums);
                if (forums != null && forums.length > 0) {
                    getTopics(forums[0].getName());
                }
            }
        });
    }

    public void onForumSelect(String forumName) {
        getTopics(forumName);
    }

    public void onTopicSelection(String topicId) {
        getPosts(topicId);
    }

    private void getPosts(String topicId) {
        postsViewHandler.showBusy();
        simpleForumService.getPosts(topicId, new AsyncCallback<Post[]>() {
            public void onFailure(Throwable caught) {
                // Show the RPC error message to the user
                Window.alert("Failed to get topics: " + caught.getMessage());
            }

            public void onSuccess(Post[] posts) {
                postsViewHandler.update(posts);
            }
        });
    }
}
