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
package org.simpledbm.network.client.impl;

import java.nio.ByteBuffer;
import java.util.Properties;

import org.simpledbm.common.api.exception.ExceptionHandler;
import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.impl.platform.PlatformImpl;
import org.simpledbm.common.util.logging.Logger;
import org.simpledbm.network.client.api.Session;
import org.simpledbm.network.client.api.SessionManager;
import org.simpledbm.network.common.api.RequestCode;
import org.simpledbm.network.common.api.SessionRequestMessage;
import org.simpledbm.network.nio.api.Connection;
import org.simpledbm.network.nio.api.NetworkUtil;
import org.simpledbm.network.nio.api.Request;
import org.simpledbm.network.nio.api.Response;
import org.simpledbm.typesystem.api.DictionaryCache;
import org.simpledbm.typesystem.api.RowFactory;
import org.simpledbm.typesystem.api.TableDefinition;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.api.TypeFactory;
import org.simpledbm.typesystem.api.TypeSystemFactory;
import org.simpledbm.typesystem.impl.TypeSystemFactoryImpl;

public class SessionManagerImpl extends SessionManager {
	
	public static final String LOGGER_NAME = "org.simpledbm.network";

    String host;
    int port;
    Connection connection;
    final TypeSystemFactory typeSystemFactory;
    final TypeFactory typeFactory;
    final DictionaryCache dictionaryCache;
    final RowFactory rowFactory;
	final Platform platform;
	final PlatformObjects po;
	final Logger log;
	final ExceptionHandler exceptionHandler;
	int timeout;

    public SessionManagerImpl(Properties properties, String host, int port, int timeout) {
    	super();
		this.platform = new PlatformImpl(properties);
		this.po = platform.getPlatformObjects(SessionManagerImpl.LOGGER_NAME);
		this.log = po.getLogger();
		this.exceptionHandler = po.getExceptionHandler();
	    this.typeSystemFactory = new TypeSystemFactoryImpl(properties, po);
	    this.typeFactory = typeSystemFactory.getDefaultTypeFactory();
        this.host = host;
        this.port = port;
        this.timeout = timeout;
        this.connection = NetworkUtil.createConnection(host, port, timeout);
        this.dictionaryCache = new DictionaryCacheProxy(this, connection, typeFactory);
        this.rowFactory = typeSystemFactory.getDefaultRowFactory(typeFactory, dictionaryCache);
    }

    public TypeFactory getTypeFactory() {
        return typeFactory;
    }

	public TableDefinition newTableDefinition(String name, int containerId,
			TypeDescriptor[] rowType) {
		return typeSystemFactory.getTableDefinition(po, typeFactory, rowFactory, containerId, name, rowType);
	}    
    
    public Session openSession() {
        SessionRequestMessage message = new SessionRequestMessage();
    	Response response = sendMessage(0, RequestCode.OPEN_SESSION, message);
        Session session = new SessionImpl(this, response.getSessionId());
        return session;
    }
    
    public Connection getConnection() {
    	return connection;
    }

    public TypeDescriptor[] getRowType(int containerId) {
    	return dictionaryCache.getTypeDescriptor(containerId);
    }
    
    public RowFactory getRowFactory() {
    	return rowFactory;
    }
    
    Response sendMessage(int sessionId, int requestCode, Storable message) {
        ByteBuffer data = ByteBuffer.allocate(message.getStoredLength());
        message.store(data);
        Request request = NetworkUtil.createRequest(data.array());
        request.setRequestCode(requestCode);
        request.setSessionId(sessionId);
        Response response = connection.submit(request);
        return response;
    }    
    
}
