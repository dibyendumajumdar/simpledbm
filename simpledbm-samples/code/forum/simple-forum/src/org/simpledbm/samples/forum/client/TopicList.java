package org.simpledbm.samples.forum.client;

import java.io.Serializable;

public class TopicList implements Serializable {
    private static final long serialVersionUID = 1L;
    Topic[] topics = new Topic[0];

    public TopicList() {
    }
    
    public void setTopics(Topic[] topics) {
        this.topics = topics;
    }
    
    public int getTopicCount() {
        return topics.length;
    }

    public Topic getTopic(int i) {
        if (i < 0 || i >= topics.length) {
            return topics[0];
        }
        return topics[i];
    }

}
