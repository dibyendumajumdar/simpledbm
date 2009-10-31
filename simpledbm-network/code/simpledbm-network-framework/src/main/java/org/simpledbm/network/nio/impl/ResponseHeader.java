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
package org.simpledbm.network.nio.impl;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ResponseHeader {
    int statusCode = 0;
    int version = 0;
    int sessionId = 0;
    int correlationId = 0;
    int dataSize = 0;

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

    static ByteBuffer allocate() {
        return ByteBuffer.allocate(24);
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
    }
}
