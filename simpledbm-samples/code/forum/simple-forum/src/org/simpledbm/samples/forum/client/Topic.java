package org.simpledbm.samples.forum.client;

import java.io.Serializable;

/**
 * A Topic represents the title of a thread of conversation. A forum can have
 * many topics. Each topic can have multiple posts.
 */
@SuppressWarnings("serial")
public class Topic implements Serializable {
    String forumName;
    long topicId;
    String title = "test";
    String startedBy = "anonymous";

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getStartedBy() {
        return startedBy;
    }

    public void setStartedBy(String startedBy) {
        this.startedBy = startedBy;
    }

    public long getTopicId() {
        return topicId;
    }

    public void setTopicId(long topicId) {
        this.topicId = topicId;
    }

    public String getForumName() {
        return forumName;
    }

    public void setForumName(String forumName) {
        this.forumName = forumName;
    }

    @Override
    public String toString() {
        return "Topic [forumName=" + forumName + ", startedBy=" + startedBy
                + ", title=" + title + ", topicId=" + topicId + "]";
    }
}
