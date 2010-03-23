package org.simpledbm.samples.forum.client;

import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;

/**
 * The client side stub for the RPC service.
 */
@RemoteServiceRelativePath("greet")
public interface SimpleForumService extends RemoteService {
  String greetServer(String name) throws IllegalArgumentException;
}
