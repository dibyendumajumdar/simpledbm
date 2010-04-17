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
                    currentTopic = topicList[0];
                    getPosts(topicList[0].getForumName(), topicList[0].getTopicId());
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
                    currentForum = forums[0];
                    getTopics(forums[0].getName());
                }
            }
        });
    }

    private void getPosts(String forumName, long topicId) {
        postsView.showBusy();
        simpleForumService.getPosts(forumName, topicId, new AsyncCallback<Post[]>() {
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
                // Show the RPC error message to the user
                Window.alert("Failed to add post: " + post + caught.getMessage());
            }

            public void onSuccess(Void discard) {
                getPosts(post.getForumName(), post.getTopicId());
            }
        });
    }

    public void saveTopic(final Topic topic, final Post post) {
        simpleForumService.saveTopic(topic, post, new AsyncCallback<Void>() {
            public void onFailure(Throwable caught) {
                // Show the RPC error message to the user
                Window.alert("Failed to add topic: " + topic + caught.getMessage());
            }

            public void onSuccess(Void discard) {
                getPosts(topic.getForumName(), topic.getTopicId());
            }
        });
    }

    public void onNewTopic() {
        new NewTopicViewImpl(currentForum, this).show();
    }

}
