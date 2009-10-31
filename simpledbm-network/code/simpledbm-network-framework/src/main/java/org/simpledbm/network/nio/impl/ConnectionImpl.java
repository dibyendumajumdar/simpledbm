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

import org.simpledbm.network.nio.api.Connection;
import org.simpledbm.network.nio.api.NetworkException;
import org.simpledbm.network.nio.api.Request;
import org.simpledbm.network.nio.api.Response;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

public class ConnectionImpl implements Connection {

    public final static String LOGGER_NAME = "org.simpledbm.network";

    Socket socket;
    InputStream is;
    OutputStream os;

    int status = OKAY;

    static final int OKAY = 0;
    static final int ERRORED = 1;
    static final int CLOSED = 2;

    public ConnectionImpl(String host, int port) {
        try {
            this.socket = new Socket(host, port);
        } catch (IOException e) {
            handleException(e);
        }
        try {
            this.is = socket.getInputStream();
        } catch (IOException e) {
            handleException(e);
        }
        try {
            this.os = socket.getOutputStream();
        } catch (IOException e) {
            handleException(e);
        }
    }

    public Response submit(Request request) {
        if (status != OKAY) {
            handleException(new IllegalStateException());
        }

        /* Send the request */
        ByteBuffer header = request.getHeaderData();
        byte[] prefix = header.array();
        try {
            os.write(prefix);
            os.write(request.getData().array());
        } catch (IOException e) {
            handleException(e);
        }

        /* Now wait/get the response */
        header = ResponseHeader.allocate();
        prefix = header.array();
        int len = 0;
        try {
            len = is.read(prefix, 0, prefix.length);
        } catch (IOException e) {
            handleException(e);
        }

        if (len != prefix.length) {
            handleException(new IllegalArgumentException());
        }

        /* Parse the response header */
        ResponseHeader responseHeader = new ResponseHeader();
        try {
            responseHeader.retrieve(header);
        } catch (IOException e1) {
            handleException(e1);
        }

        /* Now get the response data */
        len = responseHeader.getDataSize();
        byte[] data = new byte[len];
        try {
            len = is.read(data, 0, len);
        } catch (IOException e) {
            handleException(e);
        }

        if (len != data.length) {
            handleException(new IllegalArgumentException());
        }
        Response response = new ResponseImpl(responseHeader, ByteBuffer.wrap(data));
        return response;
    }

    public void close() {
        try {
            os.flush();
        } catch (IOException e) {
        }
        NIOUtil.close(os);
        try {
            while (is.available() > 0) {
                is.read();
            }
        } catch (IOException e) {
        }
        NIOUtil.close(is);
        NIOUtil.close(socket);
        status = CLOSED;
    }

    void handleException(Exception e) {
        status = ERRORED;
        NetworkException ne = new NetworkException(e);
//		logger.error(getClass().getName(), "handleException", e.getMessage(),
//				e);
        e.printStackTrace();
        throw ne;
    }

}
