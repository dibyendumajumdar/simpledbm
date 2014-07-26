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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.simpledbm.common.api.exception.SimpleDBMException;
import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.api.thread.Scheduler.Priority;
import org.simpledbm.common.util.logging.Logger;
import org.simpledbm.common.util.mcat.Message;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.common.util.mcat.MessageType;
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
    volatile boolean stop = false;
    volatile boolean opened = false;
    volatile boolean errored = false;
    final RequestHandler requestHandler;
    final PlatformObjects platformObjects;
    final Platform platform;
    final Logger log;

    static final Message m_startingIOException = new Message('N', 'F',
            MessageType.ERROR, 1,
            "IO Error occurred when attemptiong to start the server");
    static final Message m_erroredException = new Message('N', 'F',
            MessageType.ERROR, 2,
            "Unable to perform operation as server has had previous errors");
    static final Message m_alreadyStartedException = new Message('N', 'F',
            MessageType.ERROR, 3, "Server is already started");
    static final Message m_readIOException = new Message('N', 'F',
            MessageType.ERROR, 4,
            "IO Error occurred while reading from a channel");
    static final Message m_writeIOException = new Message('N', 'F',
            MessageType.ERROR, 5,
            "IO Error occurred while writing to a channel");
    static final Message m_selectIOException = new Message('N', 'F',
            MessageType.ERROR, 6,
            "IO Error occurred while selecting a channel for IO");
    static final Message m_acceptIOException = new Message('N', 'F',
            MessageType.ERROR, 7,
            "IO Error occurred while accepting a channel for IO");
    static final Message m_channelErroredException = new Message('N', 'F',
            MessageType.ERROR, 8,
            "Unable to perform {0} operation as the channel has had previous errors");
    static final Message m_shutdownException = new Message('N', 'F',
            MessageType.ERROR, 9, "An error occurred during shutdown");
    static final Message m_handlerError = new Message('N', 'F',
            MessageType.ERROR, 10,
            "An unexpected error was reported by the requestHandler");

    /**
     * Timeout for select operations; default is 10 secs.
     */
    long selectTimeout = TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);

    @Override
    public String toString() {
        return "NetworkServerImpl [socketAddress=" + serverSocketAddress + "]";
    }

    public NetworkServerImpl(Platform platform, RequestHandler requestHandler,
            Properties properties) {
        try {
            this.hostname = properties.getProperty("network.server.host",
                    "localhost");
            this.port = Integer.parseInt(properties.getProperty(
                    "network.server.port", "8000"));
            this.selectTimeout = Integer.parseInt(properties.getProperty(
                    "network.server.selectTimeout", "10000"));
            this.serverSocketAddress = new InetSocketAddress(hostname, port);
            this.platform = platform;
            this.platformObjects = platform.getPlatformObjects(LOG_NAME);
            this.log = platformObjects.getLogger();
            this.requestHandler = requestHandler;
            requestHandler.onInitialize(platform, properties);
        } catch (RuntimeException e) {
            errored = true;
            throw e;
        }
    }

    /**
     * Starts the network server.
     */
    public void start() {
        if (opened) {
            return;
        }
        if (errored) {
            throw new NetworkException(new MessageInstance(m_erroredException));
        }
        try {
            requestHandler.onStart();
            if (log.isDebugEnabled()) {
                log.debug(getClass(), "start", "Opening selector");
            }
            selector = Selector.open();
            if (log.isDebugEnabled()) {
                log.debug(getClass(), "start",
                        "Opening server socket");
            }
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            if (log.isDebugEnabled()) {
                log.debug(getClass(), "start", "Binding to "
                        + serverSocketAddress);
            }
            serverSocketChannel.socket().bind(serverSocketAddress);
            if (log.isDebugEnabled()) {
                log.debug(getClass(), "start", "Registering "
                        + serverSocketChannel + " for accepting connections");
            }
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        } catch (IOException e) {
            NIOUtil.close(selector);
            selector = null;
            NIOUtil.close(serverSocketChannel);
            serverSocketChannel = null;
            errored = true;
            throw new NetworkException(new MessageInstance(
                    m_startingIOException), e);
        }
        opened = true;
    }

    public void shutdown() {
        if (opened) {
            opened = false;
        } else {
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug(getClass(), "shutdown", "Closing server "
                    + serverSocketAddress);
        }
        stop = true;
        selector.wakeup();
        for (SelectionKey key : selector.keys()) {
            if (key.isValid() && key.attachment() != null) {
                key.cancel();
                NIOUtil.close(key.channel());
            }
        }
        NIOUtil.close(selector);
        NIOUtil.close(serverSocketChannel);
        try {
            requestHandler.onShutdown();
        } catch (RuntimeException e) {
            errored = true;
            throw e;
        }
        platformObjects.getPlatform().shutdown();
        errored = false;
    }

    public void select() {
        if (errored) {
            throw new NetworkException(new MessageInstance(m_erroredException));
        }
        if (!opened || stop) {
            return;
        }
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
        if (log.isTraceEnabled()) {
            log.trace(getClass(), "select", "Selecting events");
        }
        try {
            int n = selector.select(selectTimeout);
            if (n == 0) {
                return;
            }
        } catch (IOException e) {
            errored = true;
            throw new NetworkException(
                    new MessageInstance(m_selectIOException), e);
        }
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
    }

    private void handleWrite(SelectionKey key) {
        ProtocolHandler protocolHandler = (ProtocolHandler) key.attachment();
        if (log.isTraceEnabled()) {
            log.trace(getClass(), "handleWrite",
                    "Writing to channel " + protocolHandler.socketChannel);
        }
        protocolHandler.doWrite(key);
    }

    private void handleRead(SelectionKey key) {
        ProtocolHandler protocolHandler = (ProtocolHandler) key.attachment();
        if (log.isTraceEnabled()) {
            log.trace(getClass(), "handleWrite",
                    "Reading from channel " + protocolHandler.socketChannel);
        }
        protocolHandler.doRead(key);
    }

    private void handleAccept(SelectionKey key) {
        // For an accept to be pending the channel must be a server socket channel.
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key
                .channel();
        SocketChannel socketChannel = null;
        SelectionKey channelKey = null;
        try {
            if (log.isDebugEnabled()) {
                log.debug(getClass(), "handleWrite",
                        "Accepting new channel");
            }
            socketChannel = serverSocketChannel.accept();
            socketChannel.configureBlocking(false);
            channelKey = socketChannel.register(this.selector,
                    SelectionKey.OP_READ);
            ProtocolHandler channelHandler = new ProtocolHandler(this,
                    socketChannel);
            channelKey.attach(channelHandler);
        } catch (IOException e) {
            /*
             * If we failed to accept a new channel, we can still continue serving
             * existing channels, so do not treat this as a fatal error
             */
            log.error(getClass(), "handleAccept",
                    new MessageInstance(m_acceptIOException).toString());
            if (channelKey != null) {
                channelKey.cancel();
            }
            NIOUtil.close(socketChannel);
        }
    }

    void queueRequest(ProtocolHandler protocolHandler,
            RequestHeader requestHeader, ByteBuffer request) {
        ResponseHeader responseHeader = new ResponseHeader();
        responseHeader.setCorrelationId(requestHeader.getCorrelationId());
        RequestDispatcher requestDispatcher = new RequestDispatcher(this,
                protocolHandler, requestHandler, requestHeader, request);
        if (log.isTraceEnabled()) {
            log.trace(getClass(), "queueRequest",
                    "Scheduling request handler for channel "
                            + protocolHandler.socketChannel);
        }
        platform.getScheduler().execute(Priority.NORMAL, requestDispatcher);
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

    /**
     * A simple protocol handler. The network protocol is extremely simple. Each
     * request must have a response. The request and response packets have a
     * header and a body. The header is of fixed length. The body is variable
     * length but the length is recorded in the header so that the handler can
     * determine when a full request/response packet has been received.
     * <p>
     * 
     * @see RequestHeader
     * @see ResponseHeader
     * @author dibyendumajumdar
     * 
     */
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

        ProtocolHandler(NetworkServerImpl networkServer,
                SocketChannel socketChannel) {
            this.networkServer = networkServer;
            this.socketChannel = socketChannel;
        }

        /**
         * Perform an incremental read, keeping track of progress. When a full
         * request is detected, schedule a request handler event.
         * 
         * @param key Identifies the channel which is ready for reading
         */
        synchronized void doRead(SelectionKey key) {

            if (!okay) {
                throw new NetworkException(new MessageInstance(
                        m_channelErroredException));
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
                        readPayload = ByteBuffer.allocate(requestHeader
                                .getDataSize());
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
                        networkServer.queueRequest(this, requestHeader,
                                readPayload);
                        /* let's see if we can read another message */
                        readState = STATE_INIT;
                        readPayload = null;
                    }
                }
            } catch (IOException e) {
                networkServer.log.error(getClass(), "doRead",
                        new MessageInstance(m_readIOException, e).toString());
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

        /**
         * Perform an incremental write. Keep writing as long as the channel is
         * writable and there are more packets to be written.
         * 
         * @param key Identifies the channel that is ready for writing
         */
        synchronized void doWrite(SelectionKey key) {
            if (!okay) {
                throw new NetworkException(new MessageInstance(
                        m_channelErroredException));
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
                networkServer.log.error(getClass(), "doWrite",
                        new MessageInstance(m_writeIOException, e).toString());
                failed();
            }
        }

        /**
         * Add a write request to the queue - it will be picked by in the next
         * select loop.
         * 
         * @param wr A write request
         */
        synchronized void queueWrite(WriteRequest wr) {
            if (networkServer.log.isTraceEnabled()) {
                networkServer.log.trace(getClass(), "queueWrite",
                        "queuing write " + wr.response.limit());
            }
            wr.responseHeader.setDataSize(wr.response.limit());
            writeQueue.add(wr);
            networkServer.selector.wakeup();
        }

        /**
         * Checks whether there are queued requests to be written
         */
        synchronized boolean isWritable() {
            return writeQueue.size() > 0;
        }
    }

    /**
     * RequestDispatcher task is responsible for handling a request. Actual
     * request handling is delegated to a RequestHandler instance.
     * 
     * @author dibyendumajumdar
     */
    static final class RequestDispatcher implements Runnable {

        final NetworkServerImpl server;
        final ProtocolHandler protocolHandler;
        final RequestHeader requestHeader;
        final ByteBuffer requestData;
        final RequestHandler requestHandler;

        static final ByteBuffer defaultData = ByteBuffer.allocate(0);

        RequestDispatcher(NetworkServerImpl server,
                ProtocolHandler protocolHandler, RequestHandler requestHandler,
                RequestHeader requestHeader, ByteBuffer requestData) {
            this.server = server;
            this.protocolHandler = protocolHandler;
            this.requestHandler = requestHandler;
            this.requestHeader = requestHeader;
            this.requestData = requestData;
        }

        public void run() {
            requestData.rewind();
            Request request = new RequestImpl(requestHeader, requestData);
            // setup default response
            ResponseHeader responseHeader = new ResponseHeader();
            responseHeader.setCorrelationId(requestHeader.getCorrelationId());
            responseHeader.setStatusCode(0);
            responseHeader.setSessionId(requestHeader.getSessionId());
            responseHeader.setHasException(false);
            Response response = new ResponseImpl(responseHeader, defaultData);
            try {
                requestHandler.handleRequest(request, response);
            } catch (SimpleDBMException e) {
                server.log.error(getClass(), "run",
                        new MessageInstance(m_handlerError).toString(), e);
                responseHeader.setStatusCode(-1);
                responseHeader.setHasException(true);
                int len = e.getStoredLength();
                ByteBuffer bb = ByteBuffer.allocate(len);
                e.store(bb);
                bb.flip();
                response.setData(bb);
                responseHeader.setDataSize(len);
            } catch (Throwable e) {
                server.log.error(getClass(), "run",
                        new MessageInstance(m_handlerError).toString(), e);
                e.printStackTrace();
                responseHeader.setStatusCode(-1);
                response.setData(ByteBuffer.wrap(e.getMessage().getBytes()));
                responseHeader.setDataSize(response.getData().limit());
            }
            // TODO support the no reply option
            protocolHandler.queueWrite(new WriteRequest(responseHeader,
                    response.getData()));
        }
    }
}
