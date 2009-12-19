/***
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program; if not, write to the Free Software
 *    Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *
 *    Linking this library statically or dynamically with other modules
 *    is making a combined work based on this library. Thus, the terms and
 *    conditions of the GNU General Public License cover the whole
 *    combination.
 *
 *    As a special exception, the copyright holders of this library give
 *    you permission to link this library with independent modules to
 *    produce an executable, regardless of the license terms of these
 *    independent modules, and to copy and distribute the resulting
 *    executable under terms of your choice, provided that you also meet,
 *    for each linked independent module, the terms and conditions of the
 *    license of that module.  An independent module is a module which
 *    is not derived from or based on this library.  If you modify this
 *    library, you may extend this exception to your version of the
 *    library, but you are not obligated to do so.  If you do not wish
 *    to do so, delete this exception statement from your version.
 *
 *    Project: www.simpledbm.org
 *    Author : Dibyendu Majumdar
 *    Email  : d dot majumdar at gmail dot com ignore
 */
package org.simpledbm.network.client.api;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.simpledbm.common.api.tx.IsolationMode;
import org.simpledbm.network.common.api.EndTransactionMessage;
import org.simpledbm.network.common.api.GetTableMessage;
import org.simpledbm.network.common.api.RequestCode;
import org.simpledbm.network.common.api.SessionRequestMessage;
import org.simpledbm.network.common.api.StartTransactionMessage;
import org.simpledbm.network.nio.api.NetworkUtil;
import org.simpledbm.network.nio.api.Request;
import org.simpledbm.network.nio.api.Response;
import org.simpledbm.typesystem.api.TableDefinition;
import org.simpledbm.typesystem.api.TypeSystemFactory;

/**
 * Session represents a session with the server.
 */
public class Session {

    private final int sessionId;
    private final SessionManager sessionManager;
    
    public Session(SessionManager sessionManager, int sessionId) {
    	this.sessionManager = sessionManager;
        this.sessionId = sessionId;
    }
    
    private String getError(Response response) {
    	byte[] data = response.getData().array();
    	String msg;
		try {
			msg = new String(data, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			msg = "Error message could not be decoded";
		}
    	return msg;
    }
    
    public synchronized void close() {
        SessionRequestMessage message = new SessionRequestMessage();
        ByteBuffer data = ByteBuffer.allocate(message.getStoredLength());
        message.store(data);
        Request request = NetworkUtil.createRequest(data.array());
        request.setRequestCode(RequestCode.CLOSE_SESSION);
        request.setSessionId(getSessionId());
        Response response = getSessionManager().getConnection().submit(request);
        if (response.getStatusCode() < 0) {
            throw new SessionException("server returned error: " + getError(response));
        } 
    }
    
    public synchronized void startTransaction(IsolationMode isolationMode) {
        StartTransactionMessage message = new StartTransactionMessage(isolationMode);
        ByteBuffer data = ByteBuffer.allocate(message.getStoredLength());
        message.store(data);
        Request request = NetworkUtil.createRequest(data.array());
        request.setRequestCode(RequestCode.START_TRANSACTION);
        request.setSessionId(getSessionId());
        Response response = getSessionManager().getConnection().submit(request);
        if (response.getStatusCode() < 0) {
            throw new SessionException("server returned error: " + getError(response));
        }
    }

    private synchronized void endTransaction(boolean commit) {
        EndTransactionMessage message = new EndTransactionMessage(commit);
        ByteBuffer data = ByteBuffer.allocate(message.getStoredLength());
        message.store(data);
        Request request = NetworkUtil.createRequest(data.array());
        request.setRequestCode(RequestCode.END_TRANSACTION);
        request.setSessionId(getSessionId());
        Response response = getSessionManager().getConnection().submit(request);
        if (response.getStatusCode() < 0) {
            throw new SessionException("server returned error: " + getError(response));
        }    	
    }
    
    public void commit() {
    	endTransaction(true);
    }
    
    public void rollback() {
    	endTransaction(false);
    }
    
    public synchronized void createTable(TableDefinition tableDefinition) {
        ByteBuffer data = ByteBuffer.allocate(tableDefinition.getStoredLength());
        tableDefinition.store(data);
        byte[] arr = data.array();
        Request request = NetworkUtil.createRequest(arr);
        request.setSessionId(getSessionId());
        request.setRequestCode(RequestCode.CREATE_TABLE);
        Response response = getSessionManager().getConnection().submit(request);
        if (response.getStatusCode() < 0) {
            throw new SessionException("server returned error" + getError(response));
        }     	
    }

    public Table getTable(int containerId) {
    	GetTableMessage message = new GetTableMessage(containerId);
        ByteBuffer data = ByteBuffer.allocate(message.getStoredLength());
        message.store(data);
        Request request = NetworkUtil.createRequest(data.array());
        request.setRequestCode(RequestCode.GET_TABLE);
        request.setSessionId(getSessionId());
        Response response = getSessionManager().getConnection().submit(request);
        if (response.getStatusCode() < 0) {
            throw new SessionException("server returned error: " + getError(response));
        }    	
        TableDefinition tableDefinition = TypeSystemFactory.getTableDefinition(getSessionManager().po, getSessionManager().getTypeFactory(),
        		getSessionManager().rowFactory, response.getData());
    	return new Table(this, tableDefinition);
    }

	SessionManager getSessionManager() {
		return sessionManager;
	}

	int getSessionId() {
		return sessionId;
	}
    
}
