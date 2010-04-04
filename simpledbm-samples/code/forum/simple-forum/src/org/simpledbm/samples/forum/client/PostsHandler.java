package org.simpledbm.samples.forum.client;

public interface PostsHandler {
    void onNewPost();
    void savePost(Post post);
}
