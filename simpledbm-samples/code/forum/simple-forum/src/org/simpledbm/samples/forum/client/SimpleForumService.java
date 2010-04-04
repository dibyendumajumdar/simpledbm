package org.simpledbm.samples.forum.client;

import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;

/**
 * The client side stub for the RPC service.
 */
@RemoteServiceRelativePath("greet")
public interface SimpleForumService extends RemoteService {
  String greetServer(String name) throws IllegalArgumentException;
  Topic[] getTopics(String forumName);
  Forum[] getForums();
  Post[] getPosts(String topicId);
  void savePost(Post post);
  void saveTopic(Topic topic, Post post);
}
