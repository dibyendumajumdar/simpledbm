package org.simpledbm.samples.forum.client;

import com.google.gwt.user.client.rpc.AsyncCallback;

/**
 * The async counterpart of <code>GreetingService</code>.
 */
public interface SimpleForumServiceAsync {
    void greetServer(String input, AsyncCallback<String> callback)
            throws IllegalArgumentException;

    void getTopics(String forumName, AsyncCallback<Topic[]> callback);

    void getForums(AsyncCallback<Forum[]> callback);

    void savePost(Post post, AsyncCallback<Void> callback);

    void saveTopic(Topic topic, Post post, AsyncCallback<Void> callback);

    void getPosts(String forumName, long topicId, AsyncCallback<Post[]> callback);
}
