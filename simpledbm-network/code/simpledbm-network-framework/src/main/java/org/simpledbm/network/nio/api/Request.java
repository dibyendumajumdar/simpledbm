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

import java.nio.ByteBuffer;

/**
 * Encapsulates the request received from a network client.
 */
public interface Request {

    /**
     * Gets the session Id associated with the client.
     * If the session Id is not allocated yet, this method should return
     * 0. All valid session Ids should be > 0.
     * New session Ids should be set in the Response object.
     */
    int getSessionId();

    /**
     * Gets the correlation Id set by the network client.
     * The correlation Id should be copied to the Response object.
     */
    int getCorrelationId();

    /**
     * Gets the request code which may have been provided by the client.
     */
    int getRequestCode();

    /**
     * Sets the request code. To be used by a client generating the
     * request.
     */
    void setRequestCode(int requestCode);    
    
    /**
     * Gets the version of the request message.
     */
    int getVersion();

    /**
     *  Gets the size of the data associated with the request.
     */
    int getDataSize();

    /**
     * Gets the data sent by the client. The buffer should be independent
     * of the underlying framework, so that the request handler can manipulate this
     * if necessary. The buffer should be positioned such as position() is 0,
     * and limit() is set to the length of the buffer.
     */
    ByteBuffer getData();

    /**
     * Gets the header data.
     */
    ByteBuffer getHeaderData();
}
