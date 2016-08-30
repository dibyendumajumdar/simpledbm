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

import java.io.UnsupportedEncodingException;

import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.api.tx.IsolationMode;
import org.simpledbm.network.client.api.Session;
import org.simpledbm.network.client.api.SessionManager;
import org.simpledbm.network.client.api.Table;
import org.simpledbm.network.common.api.EndTransactionMessage;
import org.simpledbm.network.common.api.GetTableMessage;
import org.simpledbm.network.common.api.RequestCode;
import org.simpledbm.network.common.api.SessionRequestMessage;
import org.simpledbm.network.common.api.StartTransactionMessage;
import org.simpledbm.network.nio.api.Response;
import org.simpledbm.typesystem.api.TableDefinition;

/**
 * Session represents a session with the server.
 */
public class SessionImpl implements Session {

    private final int sessionId;
    final SessionManagerImpl sessionManager;

    public SessionImpl(SessionManager sessionManager, int sessionId) {
        this.sessionManager = (SessionManagerImpl) sessionManager;
        this.sessionId = sessionId;
    }

    String getError(Response response) {
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
        sendMessage(RequestCode.CLOSE_SESSION, message);
    }

    public synchronized void startTransaction(IsolationMode isolationMode) {
        StartTransactionMessage message = new StartTransactionMessage(
                isolationMode);
        sendMessage(RequestCode.START_TRANSACTION, message);
    }

    private synchronized void endTransaction(boolean commit) {
        EndTransactionMessage message = new EndTransactionMessage(commit);
        sendMessage(RequestCode.END_TRANSACTION, message);
    }

    public void commit() {
        endTransaction(true);
    }

    public void rollback() {
        endTransaction(false);
    }

    public synchronized void createTable(TableDefinition tableDefinition) {
        sendMessage(RequestCode.CREATE_TABLE, tableDefinition);
    }

    public Table getTable(int containerId) {
        GetTableMessage message = new GetTableMessage(containerId);
        Response response = sendMessage(RequestCode.GET_TABLE, message);
        TableDefinition tableDefinition = sessionManager.typeSystemFactory
                .getTableDefinition(sessionManager.po, getSessionManager()
                        .getTypeFactory(), getSessionManager().getRowFactory(),
                        response.getData());
        return new TableImpl(this, tableDefinition);
    }

    public SessionManager getSessionManager() {
        return sessionManager;
    }

    public int getSessionId() {
        return sessionId;
    }

    Response sendMessage(int requestCode, Storable message) {
        return sessionManager.sendMessage(sessionId, requestCode, message);
    }
}