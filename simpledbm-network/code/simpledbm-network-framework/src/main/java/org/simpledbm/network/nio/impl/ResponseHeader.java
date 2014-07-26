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
package org.simpledbm.network.nio.impl;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ResponseHeader {
    int statusCode = 0;
    int version = 0;
    int sessionId = 0;
    int correlationId = 0;
    int dataSize = 0;
    boolean hasException = false;

    public int getStatusCode() {
        return statusCode;
    }

    public int getSessionId() {
        return sessionId;
    }

    public void setSessionId(int sessionId) {
        this.sessionId = sessionId;
    }

    public int getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(int correlationId) {
        this.correlationId = correlationId;
    }

    public void setStatusCode(int requestCode) {
        this.statusCode = requestCode;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getDataSize() {
        return dataSize;
    }

    public void setDataSize(int dataSize) {
        this.dataSize = dataSize;
    }

    public boolean hasException() {
        return hasException;
    }

    public void setHasException(boolean hasException) {
        this.hasException = hasException;
    }

    static ByteBuffer allocate() {
        return ByteBuffer.allocate(25);
    }

    void store(ByteBuffer bb) {
        bb.put((byte) 'S');
        bb.put((byte) 'd');
        bb.put((byte) 'B');
        bb.put((byte) 'm');
        bb.putInt(version);
        bb.putInt(sessionId);
        bb.putInt(correlationId);
        bb.putInt(statusCode);
        bb.putInt(dataSize);
        bb.put((byte) (hasException ? 1 : 0));
    }

    void retrieve(ByteBuffer bb) throws IOException {
        byte c1 = bb.get();
        byte c2 = bb.get();
        byte c3 = bb.get();
        byte c4 = bb.get();
        if (c1 != 'S' || c2 != 'd' || c3 != 'B' || c4 != 'm') {
            throw new IOException("Invalid header");
        }
        version = bb.getInt();
        sessionId = bb.getInt();
        correlationId = bb.getInt();
        statusCode = bb.getInt();
        dataSize = bb.getInt();
        byte b = bb.get();
        hasException = (b == 1) ? true : false;
    }
}
