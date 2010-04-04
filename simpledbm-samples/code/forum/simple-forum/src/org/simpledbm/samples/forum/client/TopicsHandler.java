package org.simpledbm.samples.forum.client;

public interface TopicsHandler {

    void onTopicSelection(String topicId);

    void saveTopic(Topic topic, Post post);
    void onNewTopic();
}
