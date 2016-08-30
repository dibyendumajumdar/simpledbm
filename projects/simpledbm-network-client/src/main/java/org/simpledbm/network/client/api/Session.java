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
package org.simpledbm.network.client.api;

import org.simpledbm.common.api.tx.IsolationMode;
import org.simpledbm.typesystem.api.TableDefinition;

/**
 * A Session encapsulates an interactive session with the server. Each session
 * can only have one active transaction at any point in time. Clients can open
 * multiple simultaneous sessions.
 * <p>
 * All sessions created by a SessionManager share a single network connection
 * to the server.
 */
public interface Session {

    /**
     * Closes the session. If there is any outstanding transaction, it will be
     * aborted. Sessions should be closed by client applications when no longer
     * required, as this will free up resources on the server.
     */
    public void close();

    /**
     * Starts a new transaction. In the context of a session, only one
     * transaction can be active at a point in time, hence if this method will
     * fail if there is already an active transaction.
     * 
     * @param isolationMode Lock isolation mode for the transaction
     */
    public void startTransaction(IsolationMode isolationMode);

    /**
     * Commits the current transaction; an exception will be thrown if there is
     * no active transaction.
     */
    public void commit();

    /**
     * Aborts the current transaction; an exception will be thrown if there is
     * no active transaction
     */
    public void rollback();

    /**
     * Creates a table as specified. The table will be created using its own
     * transaction independent of the transaction managed by the session.
     * 
     * @param tableDefinition The TableDefinition
     */
    public void createTable(TableDefinition tableDefinition);

    /**
     * Obtains a reference to the table. The Table container will be locked in
     * SHARED mode.
     * 
     * @param containerId The ID of the table's container
     * @return A Table object
     */
    public Table getTable(int containerId);

    /**
     * Gets the SessionManager that is managing this session.
     */
    public SessionManager getSessionManager();

    /**
     * Gets the unique id associated with this session.
     */
    public int getSessionId();

}
