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
package org.simpledbm.network.nio.api;

import org.simpledbm.common.api.platform.Platform;

import java.util.Properties;

/**
 * RequestHandler interface specifies the contract between the framework and the
 * business logic. The framework supplies the Request received from the client,
 * and provides a placeholder Response object for the handler to set the reply
 * message.
 * <p>
 * The request handler must operate in a thread safe manner. The framework
 * reuses the same instance of the RequestHandler instance for handling all
 * requests across multiple threads. Hence the RequestHandler must ensure that
 * any data that is not local to the handler's stack is synchronized.
 * <p>
 * The handler does not need to synchronize the the request and response
 * objects.
 * <p>
 * Errors should preferably be communicated by setting the status code to a
 * negative value. If an exception is thrown, the framework will catch it and
 * set the status code to -1. Note that if an exception is thrown the framework
 * may discard the data in the response object, and replace it with details of
 * the exception.
 */
public interface RequestHandler {

    /**
     * This method will be invoked by the framework when the server is
     * initialized.
     */
    void onInitialize(Platform platform, Properties properties);

    /**
     * This method will be invoked by the framework when the server opens for
     * business.
     */
    void onStart();

    /**
     * Should perform the required business logic and set the Response object
     * values.
     * 
     * @param request The Request supplied by the client.
     * @param response The Response to be sent back to the client.
     */
    void handleRequest(Request request, Response response);

    /**
     * This method will be invoked by the framework when the server is shutting
     * down. Before this method is invoked, all
     */
    void onShutdown();
}
