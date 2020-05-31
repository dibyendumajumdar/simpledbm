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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.simpledbm.common.api.exception.SimpleDBMException;
import org.simpledbm.common.util.mcat.Message;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.common.util.mcat.MessageType;
import org.simpledbm.network.nio.api.Connection;
import org.simpledbm.network.nio.api.NetworkException;
import org.simpledbm.network.nio.api.Request;
import org.simpledbm.network.nio.api.Response;

public class ConnectionImpl implements Connection {

    public final static String LOGGER_NAME = "org.simpledbm.network";

    Socket socket;
    InputStream is;
    OutputStream os;

    int status = OKAY;

    static final int OKAY = 0;
    static final int ERRORED = 1;
    static final int CLOSED = 2;

    static final Message m_IOException = new Message('N', 'C',
            MessageType.ERROR, 1,
            "An IO Error occurred while performing {0} operation");
    static final Message m_erroredException = new Message('N', 'C',
            MessageType.ERROR, 2,
            "Unable to perform operation as the connection has had previous errors");
    static final Message m_invalidResponseHeaderException = new Message('N',
            'C', MessageType.ERROR, 3,
            "Invalid response header, length received does not match expected length");
    static final Message m_invalidResponseDataException = new Message('N', 'C',
            MessageType.ERROR, 4,
            "Invalid response data, length received does not match expected length");
    static final Message m_serverError = new Message('N', 'C',
            MessageType.ERROR, 5, "Server returned error message: {0}");

    private String getError(Response response) {
        byte[] data = response.getData().array();
        String msg;
        msg = new String(data, StandardCharsets.UTF_8);
        return msg;
    }

    public ConnectionImpl(String host, int port, int timeout) {
        try {
            this.socket = new Socket(host, port);
            socket.setSoTimeout(timeout);
        } catch (IOException e) {
            handleException(e, "connect");
        }
        try {
            this.is = socket.getInputStream();
        } catch (IOException e) {
            handleException(e, "connect");
        }
        try {
            this.os = socket.getOutputStream();
        } catch (IOException e) {
            handleException(e, "connect");
        }
    }

    public final synchronized Response submit(Request request) {
        if (status != OKAY) {
            throw new NetworkException(new MessageInstance(m_erroredException,
                    "submit"));
        }

        /* Send the request */
        ByteBuffer header = request.getHeaderData();
        byte[] prefix = header.array();
        try {
            os.write(prefix);
            os.write(request.getData().array());
        } catch (IOException e) {
            handleException(e, "submit");
        }

        /* Now wait/get the response */
        header = ResponseHeader.allocate();
        prefix = header.array();
        int len = 0;
        try {
            len = is.read(prefix, 0, prefix.length);
        } catch (IOException e) {
            handleException(e, "submit");
        }

        if (len != prefix.length) {
            status = ERRORED;
            throw new NetworkException(new MessageInstance(
                    m_invalidResponseHeaderException));
        }

        /* Parse the response header */
        ResponseHeader responseHeader = new ResponseHeader();
        try {
            responseHeader.retrieve(header);
        } catch (IOException e1) {
            handleException(e1, "submit");
        }

        /* Now get the response data */
        len = responseHeader.getDataSize();
        byte[] data = new byte[len];
        try {
            len = is.read(data, 0, len);
        } catch (IOException e) {
//            System.err.println("Expected length = " + len);
            handleException(e, "submit");
        }

        if (len != data.length) {
            status = ERRORED;
            throw new NetworkException(new MessageInstance(
                    m_invalidResponseDataException));
        }
        Response response = new ResponseImpl(responseHeader, ByteBuffer
                .wrap(data));
        if (response.getStatusCode() == -1) {
            if (responseHeader.hasException()) {
                // We try to propagate an exception raised by the server
                throw new SimpleDBMException(response.getData());
            } else {
                // error cannot be propagated; so throw a new exception
                throw new NetworkException(new MessageInstance(m_serverError,
                        getError(response)));
            }
        }
        return response;
    }

    public final synchronized void close() {
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

    private void handleException(Exception e, String op) {
        status = ERRORED;
        NetworkException ne = new NetworkException(new MessageInstance(
                m_IOException, op), e);
        throw ne;
    }

}
