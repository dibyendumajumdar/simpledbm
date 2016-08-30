/**
 * DO NOT REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Contributor(s):
 *
 * The Original Software is SimpleDBM (www.simpledbm.org).
 * The Initial Developer of the Original Software is Dibyendu Majumdar.
 *
 * Portions Copyright 2005-2014 Dibyendu Majumdar. All Rights Reserved.
 *
 * The contents of this file are subject to the terms of the
 * Apache License Version 2 (the "APL"). You may not use this
 * file except in compliance with the License. A copy of the
 * APL may be obtained from:
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the APL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the APL, the GPL or the LGPL.
 *
 * Copies of GPL and LGPL may be obtained from:
 * http://www.gnu.org/licenses/license-list.html
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

    final String host;
    final int port;
    final Connection connection;
    final TypeSystemFactory typeSystemFactory;
    final TypeFactory typeFactory;
    final DictionaryCache dictionaryCache;
    final RowFactory rowFactory;
    final Platform platform;
    final PlatformObjects po;
    final Logger log;
    final ExceptionHandler exceptionHandler;
    final int timeout;

    public SessionManagerImpl(Properties properties, String host, int port,
            int timeout) {
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
        this.dictionaryCache = new DictionaryCacheProxy(this, connection,
                typeFactory);
        this.rowFactory = typeSystemFactory.getDefaultRowFactory(typeFactory,
                dictionaryCache);
    }

    public TypeFactory getTypeFactory() {
        return typeFactory;
    }

    public TableDefinition newTableDefinition(String name, int containerId,
            TypeDescriptor[] rowType) {
        return typeSystemFactory.getTableDefinition(po, typeFactory,
                rowFactory, containerId, name, rowType);
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

    @Override
    public void close() {
        if (connection != null) {
            connection.close();
        }
        // FIXME we need to avoid starting the scheduler - until then
        // as a workaround we shutdown the platform.
        platform.shutdown();
    }

}
