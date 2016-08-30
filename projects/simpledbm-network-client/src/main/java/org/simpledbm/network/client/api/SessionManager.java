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

import java.util.Properties;

import org.simpledbm.network.client.impl.SessionManagerImpl;
import org.simpledbm.network.nio.api.Connection;
import org.simpledbm.typesystem.api.RowFactory;
import org.simpledbm.typesystem.api.TableDefinition;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.api.TypeFactory;

/**
 * The SessionManager manages the connection to the SimpleDBM Network Server,
 * and initiates sessions used by the clients. Each SessionManager maintains
 * a single connection to the server. Requests sent over a single connection
 * are serialized.
 * 
 * @author dibyendu majumdar
 */
public abstract class SessionManager {

    /**
     * Obtains an instance of the SessionManager for the specified connection
     * parameters. The client should allow for the fact that the returned
     * instance may be a shared one.
     * 
     * @param properties A set of properties - at present only logging parameters
     *                   are used
     * @param host       The DNS name or IP address of the server
     * @param port       The port the server is listening on
     * @param timeout    The socket timeout in milliseconds. This is the
     *                   timeout for read/write operations.
     * @return A Session Manager object
     */
    public static SessionManager getSessionManager(Properties properties,
            String host, int port, int timeout) {
        return new SessionManagerImpl(properties, host, port, timeout);
    }

    /**
     * Gets the TypeFactory associated with the database.
     * 
     * @return
     */
    public abstract TypeFactory getTypeFactory();

    /**
     * Gets the RowFactory for the database.
     * 
     * @return
     */
    public abstract RowFactory getRowFactory();

    /**
     * Creates a new TableDefinition.
     * 
     * @param name Name of the table's container
     * @param containerId ID of the container; must be unique
     * @param rowType The row definition as an arry of TypeDescriptors
     * @return A TableDefinition object
     */
    public abstract TableDefinition newTableDefinition(String name,
            int containerId, TypeDescriptor[] rowType);

    /**
     * Starts a new session.
     * 
     * @return
     */
    public abstract Session openSession();

    /**
     * Gets the underlying connection object associated with this
     * SessionManager.
     * <p>
     * The connection object must be handled with care, as its correct operation
     * is vital to the client server communication.
     * 
     * @return
     */
    public abstract Connection getConnection();

    /**
     * Closes the SessionManager and its connection with the database,
     * releasing any acquired resources.
     */
    public abstract void close();
}
