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

import java.nio.ByteBuffer;

/**
 * Encapsulates the request received from a network client.
 */
public interface Request {

    /**
     * Gets the session Id associated with the client. If the session Id is not
     * allocated yet, this method should return 0. All valid session Ids should
     * be &gt; 0. New session Ids should be set in the Response object.
     */
    int getSessionId();

    /**
     * Gets the correlation Id set by the network client. The correlation Id
     * should be copied to the Response object.
     */
    int getCorrelationId();

    /**
     * Gets the request code which may have been provided by the client.
     */
    int getRequestCode();

    /**
     * Sets the request code. To be used by a client generating the request.
     */
    void setRequestCode(int requestCode);

    /**
     * Sets the session ID that is generating this request.
     */
    void setSessionId(int sessionId);

    /**
     * Gets the version of the request message.
     */
    int getVersion();

    /**
     * Gets the size of the data associated with the request.
     */
    int getDataSize();

    /**
     * Gets the data sent by the client. The buffer should be independent of the
     * underlying framework, so that the request handler can manipulate this if
     * necessary. The buffer should be positioned such as position() is 0, and
     * limit() is set to the length of the buffer.
     */
    ByteBuffer getData();

    /**
     * Gets the header data.
     */
    ByteBuffer getHeaderData();
}
