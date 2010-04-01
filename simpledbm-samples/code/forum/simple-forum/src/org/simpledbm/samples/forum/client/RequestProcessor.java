package org.simpledbm.samples.forum.client;

import com.google.gwt.core.client.GWT;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.rpc.AsyncCallback;

public class RequestProcessor {

    interface TopicListHandler {
        void update(TopicList topicList);

        void showBusy();
    }

    interface ForumsHandler {
        void update(Forum[] forums);
        void showBusy();
    }
    
    /**
     * Create a remote service proxy to talk to the server-side Greeting
     * service.
     */
    private final SimpleForumServiceAsync simpleForumService = GWT
            .create(SimpleForumService.class);

    TopicListHandler topicListHandler;
    ForumsHandler forumsHandler;

    public RequestProcessor(TopicListHandler topicListHandler, ForumsHandler forumsHandler) {
        this.topicListHandler = topicListHandler;
        this.forumsHandler = forumsHandler;
    }
    
    void getTopics(String forumName) {
        topicListHandler.showBusy();
        simpleForumService.getTopics(forumName, new AsyncCallback<TopicList>() {
            public void onFailure(Throwable caught) {
                // Show the RPC error message to the user
                Window.alert("Failed to get topics: " + caught.getMessage());
            }

            public void onSuccess(TopicList topicList) {
                topicListHandler.update(topicList);
            }
        });
    }

    void getForums() {
        forumsHandler.showBusy();
        topicListHandler.showBusy();
        simpleForumService.getForums(new AsyncCallback<Forum[]>() {
            public void onFailure(Throwable caught) {
                // Show the RPC error message to the user
                Window.alert("Failed to get forums: " + caught.getMessage());
            }

            public void onSuccess(Forum[] forums) {
                forumsHandler.update(forums);
                if (forums != null && forums.length > 0) {
                    getTopics(forums[0].getName());
                }
            }
        });
    }
}
