package org.simpledbm.network.common.api;

import java.nio.ByteBuffer;

import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.util.TypeSize;

public class QueryDictionaryMessage implements Storable {
	
	public int containerId;

    public QueryDictionaryMessage(int containerId) {
    	this.containerId = containerId;
    }
    
    public QueryDictionaryMessage(ByteBuffer bb) {
    	containerId = bb.getInt();
    }
    
    public int getStoredLength() {
        return TypeSize.INTEGER;
    }

    public void store(ByteBuffer bb) {
    	bb.putInt(containerId);
    }

}
