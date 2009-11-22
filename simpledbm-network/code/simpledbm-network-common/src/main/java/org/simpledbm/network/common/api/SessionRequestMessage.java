package org.simpledbm.network.common.api;

import java.nio.ByteBuffer;

import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.api.registry.StorableFactory;

public class SessionRequestMessage implements Storable {

    public SessionRequestMessage() {
    }
    
    SessionRequestMessage(ByteBuffer bb) {
    }
    
    public int getStoredLength() {
        return 0;
    }

    public void store(ByteBuffer arg0) {
    }

    public static class SessionRequestFactory implements StorableFactory {
        public Storable getStorable(ByteBuffer bb) {
            return new SessionRequestMessage(bb);
        }
    }
    
}
