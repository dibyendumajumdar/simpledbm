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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.util.logging.Logger;
import org.simpledbm.network.nio.api.NetworkException;
import org.simpledbm.network.nio.api.NetworkServer;
import org.simpledbm.network.nio.api.Request;
import org.simpledbm.network.nio.api.RequestHandler;
import org.simpledbm.network.nio.api.Response;

public class NetworkServerImpl implements NetworkServer {

    public static final String LOG_NAME = "org.simpledbm.network";

    final String hostname;
    final int port;
    final InetSocketAddress serverSocketAddress;
    ServerSocketChannel serverSocketChannel;
    Selector selector;
    ExecutorService requestHandlerService;
    volatile boolean stop = false;
    volatile boolean opened = false;
    final RequestHandler requestHandler;
    final PlatformObjects platformObjects;
    final Logger log;

    /**
     * Timeout for select operations; default is 10 secs.
     */
    long selectTimeout = TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);

    @Override
    public String toString() {
        return "NetworkServerImpl [socketAddress=" + serverSocketAddress + "]";
    }

    public NetworkServerImpl(Platform platform, RequestHandler requestHandler, Properties properties) {
        this.hostname = properties.getProperty("network.server.host", "localhost");
        this.port = Integer.parseInt(properties.getProperty("network.server.port", "8000"));
        this.serverSocketAddress = new InetSocketAddress(hostname, port);
        this.platformObjects = platform.getPlatformObjects(LOG_NAME);
        this.log = platformObjects.getLogger();
        this.requestHandler = requestHandler;
        requestHandler.onInitialize(platform, properties);
    }

    /**
     * Starts the network server.
     */
    public void start() {
        try {
            requestHandler.onStart();
//        	System.err.println("Opening selector");
            selector = Selector.open();
//        	System.err.println("Opening server socket");
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
//        	System.err.println("Binding to " + serverSocketAddress);
            serverSocketChannel.socket().bind(serverSocketAddress);
//        	System.err.println("Registering " + serverSocketChannel + " for accepting connections");
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        } catch (Exception e) {
            NIOUtil.close(selector);
            selector = null;
            NIOUtil.close(serverSocketChannel);
            serverSocketChannel = null;
            throw new NetworkException(e);
        }
        requestHandlerService = Executors.newFixedThreadPool(1);
        opened = true;
    }

    public void shutdown() {
        if (opened) {
            opened = false;
        } else {
            return;
        }
        System.err.println("Closing server " + serverSocketAddress);
        stop = true;
        selector.wakeup();
        requestHandlerService.shutdown();
        for (SelectionKey key : selector.keys()) {
            if (key.isValid() && key.attachment() != null) {
                key.cancel();
                NIOUtil.close(key.channel());
            }
        }
        NIOUtil.close(selector);
        NIOUtil.close(serverSocketChannel);
        requestHandler.onShutdown();
    }

    public void select() {
        if (!opened || stop) {
            return;
        }
        try {
            for (SelectionKey key : selector.keys()) {
                if (!key.isValid()) {
                    continue;
                }
                ProtocolHandler handler = (ProtocolHandler) key.attachment();
                if (handler == null) {
                    /*
                     * Must be the serverSocketChannel which doesn't have an
                     * attached handler.
                     */
                    continue;
                }
                if (!handler.isOkay()) {
                    /*
                     * Handler has errored or the client has closed connection.
                     */
                    key.cancel();
                    NIOUtil.close(key.channel());
                    continue;
                }
                if (handler.isWritable()) {
                    key.interestOps(SelectionKey.OP_WRITE);
                } else {
                    key.interestOps(SelectionKey.OP_READ);
                }
            }
//        	System.err.println("Selecting events");
            selector.select(selectTimeout);
            Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
            while (iter.hasNext()) {
                SelectionKey key = iter.next();
                iter.remove();

                if (!key.isValid()) {
                    continue;
                }
                if (key.isAcceptable()) {
                    handleAccept(key);
                } else if (key.isReadable()) {
                    handleRead(key);
                } else if (key.isWritable()) {
                    handleWrite(key);
                }
            }
        } catch (IOException e) {
            throw new NetworkException(e);
        }
    }

    private void handleWrite(SelectionKey key) {
        ProtocolHandler protocolHandler = (ProtocolHandler) key.attachment();
//    	System.err.println("Writing to channel " + protocolHandler.socketChannel);
        protocolHandler.doWrite(key);
    }

    private void handleRead(SelectionKey key) {
        ProtocolHandler protocolHandler = (ProtocolHandler) key.attachment();
//    	System.err.println("Reading from channel " + protocolHandler.socketChannel);
        protocolHandler.doRead(key);
    }

    private void handleAccept(SelectionKey key) throws IOException {
        // For an accept to be pending the channel must be a server socket channel.
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
        SocketChannel socketChannel = null;
        try {
//        	System.err.println("Accepting new channel");
            socketChannel = serverSocketChannel.accept();
            socketChannel.configureBlocking(false);
            SelectionKey channelKey = socketChannel.register(this.selector, SelectionKey.OP_READ);
            ProtocolHandler channelHandler = new ProtocolHandler(this, socketChannel);
            channelKey.attach(channelHandler);
        } catch (IOException e) {
            NIOUtil.close(socketChannel);
            throw e;
        }
    }

    void queueRequest(ProtocolHandler protocolHandler, RequestHeader requestHeader, ByteBuffer request) {
        ResponseHeader responseHeader = new ResponseHeader();
        responseHeader.setCorrelationId(requestHeader.getCorrelationId());
        RequestDispatcher requestDispatcher = new RequestDispatcher(protocolHandler, requestHandler, requestHeader, request, responseHeader);
//    	System.err.println("Scheduling request handler for channel " + protocolHandler.socketChannel);
        requestHandlerService.submit(requestDispatcher);
    }

    public void requestStop() {
        if (isOpen()) {
            stop = true;
            selector.wakeup();
        }
    }

    public boolean isOpen() {
        return opened && !stop;
    }

    static final class WriteRequest {
        final ResponseHeader responseHeader;
        final ByteBuffer response;

        WriteRequest(ResponseHeader responseHeader, ByteBuffer response) {
            super();
            this.responseHeader = responseHeader;
            this.response = response;
        }

        ResponseHeader getResponseHeader() {
            return responseHeader;
        }

        ByteBuffer getResponse() {
            return response;
        }
    }

    static final class ProtocolHandler {
        final NetworkServerImpl networkServer;
        final SocketChannel socketChannel;

        static final int STATE_INIT = 0;
        static final int STATE_HEADER = 1;
        static final int STATE_HEADER_COMPLETED = 2;
        static final int STATE_PAYLOAD = 3;
        static final int STATE_PAYLOAD_COMPLETED = 3;

        ByteBuffer readHeader = RequestHeader.allocate();
        RequestHeader requestHeader = new RequestHeader();
        ByteBuffer readPayload = null;
        int readState = STATE_INIT;

        ByteBuffer writeHeader = ResponseHeader.allocate();
        ArrayList<WriteRequest> writeQueue = new ArrayList<WriteRequest>();
        WriteRequest current = null;
        int writeState = STATE_INIT;

        boolean okay = true;

        ProtocolHandler(NetworkServerImpl networkServer, SocketChannel socketChannel) {
            this.networkServer = networkServer;
            this.socketChannel = socketChannel;
        }

        synchronized void doRead(SelectionKey key) {

            if (!okay) {
                throw new IllegalStateException("Channel is in error");
            }
            try {
                while (true) {
                    /* We read as much as we can */
                    if (readState == STATE_INIT) {
                        /* Initial state */
                        readHeader.clear();
                        requestHeader = new RequestHeader();
                        int n = socketChannel.read(readHeader);
                        if (n < 0) {
                            eof();
                            break;
                        }
                        if (readHeader.remaining() == 0) {
                            /* We got everything we need */
                            readState = STATE_HEADER_COMPLETED;
                        } else {
                            /* Need to resume reading the header some other time */
                            readState = STATE_HEADER;
                            break;
                        }
                    }

                    if (readState == STATE_HEADER) {
                        /* Resume reading header */
                        int n = socketChannel.read(readHeader);
                        if (n < 0) {
                            eof();
                            break;
                        }
                        if (readHeader.remaining() == 0) {
                            /* We got everything we need */
                            readState = STATE_HEADER_COMPLETED;
                        } else {
                            /* Need to resume reading the header some other time */
                            break;
                        }
                    }

                    if (readState == STATE_HEADER_COMPLETED) {
                        /* parse the header */
                        readHeader.rewind();
                        requestHeader.retrieve(readHeader);
                        /* allocate buffer for reading the payload */
                        readPayload = ByteBuffer.allocate(requestHeader.getDataSize());
                        readState = STATE_PAYLOAD;
                    }

                    if (readState == STATE_PAYLOAD) {
                        /* get the payload */
                        int n = socketChannel.read(readPayload);
                        if (n < 0) {
                            eof();
                            break;
                        }
                        if (readPayload.remaining() == 0) {
                            /* we got the payload */
                            readState = STATE_PAYLOAD_COMPLETED;
                        } else {
                            /* still more to read, must resume later */
                            break;
                        }
                    }

                    if (readState == STATE_PAYLOAD_COMPLETED) {
                        /* read completed, queue the request */
                        networkServer.queueRequest(this, requestHeader, readPayload);
                        /* let's see if we can read another message */
                        readState = STATE_INIT;
                        readPayload = null;
                    }
                }
            } catch (IOException e) {
                // FIXME log error
                failed();
            }
        }

        void eof() {
            okay = false;
        }

        void failed() {
            okay = false;
        }

        boolean isOkay() {
            return okay;
        }

        synchronized void doWrite(SelectionKey key) {
            if (!okay) {
                throw new IllegalStateException("Channel is in error");
            }
            try {
                while (true) {
                    /* Keep writing as long as we can */
                    if (current == null) {
                        /* Get the next message */
                        if (writeQueue.size() > 0) {
                            current = writeQueue.remove(0);
                        } else {
                            /* No more messages to write */
                            break;
                        }
                    }
                    if (writeState == STATE_INIT) {
                        writeHeader.clear();
                        current.getResponseHeader().store(writeHeader);
                        writeHeader.flip();
                        socketChannel.write(writeHeader);
                        if (writeHeader.remaining() == 0) {
                            /* done writing the header */
                            writeState = STATE_PAYLOAD;
                        } else {
                            /* need to resume write at a later time */
                            writeState = STATE_HEADER;
                            break;
                        }
                    }

                    if (writeState == STATE_HEADER) {
                        /* resume writing the header */
                        socketChannel.write(writeHeader);
                        if (writeHeader.remaining() == 0) {
                            /* done writing the header */
                            writeState = STATE_PAYLOAD;
                        } else {
                            /* need to resume write at a leter time */
                            break;
                        }
                    }

                    if (writeState == STATE_PAYLOAD) {
                        /* write the payload */
                        socketChannel.write(current.getResponse());
                        if (current.getResponse().remaining() == 0) {
                            /* done */
                            writeState = STATE_PAYLOAD_COMPLETED;
                        } else {
                            /* need to resume at a later time */
                            break;
                        }
                    }

                    if (writeState == STATE_PAYLOAD_COMPLETED) {
                        /* all done so let's write another message */
                        writeState = STATE_INIT;
                        current = null;
                    }
                }
            } catch (IOException e) {
                // FIXME log error
                failed();
            }
        }

        synchronized void queueWrite(WriteRequest wr) {
            try {
                wr.responseHeader.setDataSize(wr.response.limit());
            }
            catch (NullPointerException e) {
                e.printStackTrace();
            }
            writeQueue.add(wr);
            networkServer.selector.wakeup();
        }

        synchronized boolean isWritable() {
            return writeQueue.size() > 0;
        }
    }

    static final class RequestDispatcher implements Callable<Object> {

        final ProtocolHandler protocolHandler;
        final RequestHeader requestHeader;
        final ResponseHeader responseHeader;
        final ByteBuffer data;

        final RequestHandler requestHandler;

        RequestDispatcher(ProtocolHandler protocolHandler, RequestHandler requestHandler, RequestHeader requestHeader, ByteBuffer request, ResponseHeader responseHeader) {
            this.protocolHandler = protocolHandler;
            this.requestHandler = requestHandler;
            this.requestHeader = requestHeader;
            this.responseHeader = responseHeader;
            this.data = request;
        }

        public Object call() throws Exception {
            data.rewind();
            Request request = new RequestImpl(requestHeader, data);
            Response response = new ResponseImpl(responseHeader, null);
            try {
                requestHandler.handleRequest(request, response);
            }
            catch (RuntimeException e) {
                responseHeader.setStatusCode(-1);
                response.setData(ByteBuffer.wrap(e.getMessage().getBytes()));
                responseHeader.setDataSize(response.getData().limit());
            }
            // TODO support the no reply option
            protocolHandler.queueWrite(new WriteRequest(responseHeader, response.getData()));
            return null;
        }
    }
}

