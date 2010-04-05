package org.simpledbm.samples.forum.client;

public interface TopicsHandler {

    void onTopicSelection(Topic topic);

    void saveTopic(Topic topic, Post post);

    void onNewTopic();
}
