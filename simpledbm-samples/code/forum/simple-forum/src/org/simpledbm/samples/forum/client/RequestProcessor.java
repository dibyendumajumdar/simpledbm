package org.simpledbm.samples.forum.client;

import com.google.gwt.core.client.GWT;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.rpc.AsyncCallback;

public class RequestProcessor implements ForumsHandler, TopicsHandler {

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

    /**
     * Create a remote service proxy to talk to the server-side Greeting
     * service.
     */
    private final SimpleForumServiceAsync simpleForumService = GWT
            .create(SimpleForumService.class);

    TopicsViewHandler topicsViewHandler;
    ForumsViewHandler forumsViewHandler;

    public RequestProcessor(TopicsViewHandler topicListHandler,
            ForumsViewHandler forumsHandler) {
        this.topicsViewHandler = topicListHandler;
        this.forumsViewHandler = forumsHandler;
        forumsHandler.setForumsHandler(this);
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
            }
        });
    }

    void getForums() {
        forumsViewHandler.showBusy();
        topicsViewHandler.showBusy();
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
    }
}
