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
package org.simpledbm.network.nio.api;

import org.simpledbm.common.api.platform.Platform;

import java.util.Properties;

/**
 * RequestHandler interface specifies the contract between the framework and
 * the business logic. The framework supplies the Request received from the
 * client, and provides a placeholder Response object for the handler to set
 * the reply message.
 * <p>
 * The request handler must operate in a thread safe manner. The framework
 * reuses the same instance of the RequestHandler instance for handling all requests across
 * multiple threads. Hence the RequestHandler must ensure that any data that is
 * not local to the handler's stack is synchronized.
 * <p>
 * The handler does not need to synchronize the the request and response objects.
 * <p>
 * Errors should preferably be communicated by setting the status code to a
 * negative value. If an exception is thrown, the framework will catch it and
 * set the status code to -1. Note that if an exception is thrown the framework
 * may discard the data in the response object, and replace it with details of
 * the exception.
 */
public interface RequestHandler {

    /**
     * This method will be invoked by the framework when the server is initialized.
     * @param properties
     */
    void onInitialize(Platform platform, Properties properties);

    /**
     * This method will be invoked by the framework when the server opens
     * for business.
     */
    void onStart();

    /**
     * Should perform the required business logic and set the Response object
     * values.
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
