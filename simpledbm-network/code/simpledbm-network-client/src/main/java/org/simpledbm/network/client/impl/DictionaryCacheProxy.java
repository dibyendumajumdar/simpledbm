package org.simpledbm.network.client.impl;

import java.nio.ByteBuffer;

import org.simpledbm.network.client.api.SessionException;
import org.simpledbm.network.common.api.QueryDictionaryMessage;
import org.simpledbm.network.common.api.RequestCode;
import org.simpledbm.network.nio.api.Connection;
import org.simpledbm.network.nio.api.NetworkUtil;
import org.simpledbm.network.nio.api.Request;
import org.simpledbm.network.nio.api.Response;
import org.simpledbm.typesystem.api.DictionaryCache;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.api.TypeFactory;

public class DictionaryCacheProxy implements DictionaryCache {
	
	Connection connection;
	TypeFactory typeFactory;
	
	public DictionaryCacheProxy(Connection connection, TypeFactory typeFactory) {
		this.connection = connection;
		this.typeFactory = typeFactory;
	}

	public TypeDescriptor[] getTypeDescriptor(int containerId) {
		System.err.println("Sending query to server");
        QueryDictionaryMessage message = new QueryDictionaryMessage(containerId);
        ByteBuffer data = ByteBuffer.allocate(message.getStoredLength());
        message.store(data);
        Request request = NetworkUtil.createRequest(data.array());
        request.setRequestCode(RequestCode.QUERY_DICTIONARY);
        Response response = connection.submit(request);
        if (response.getStatusCode() < 0) {
            throw new SessionException("server returned error");
        }
        TypeDescriptor[] td = typeFactory.retrieve(response.getData());
        return td;
	}

	public void registerRowType(int containerId, TypeDescriptor[] rowTypeDesc) {
		throw new UnsupportedOperationException();
	}

	public void unregisterRowType(int containerId) {
		throw new UnsupportedOperationException();
	}

}
