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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.simpledbm.network.nio.api.NetworkException;
import org.simpledbm.network.nio.api.Request;
import org.simpledbm.network.nio.api.RequestHandler;
import org.simpledbm.network.nio.api.Response;
import org.simpledbm.network.nio.samples.EchoRequestHandler;

public class NetworkServer {
	
    final InetSocketAddress serverSocketAddress;
    ServerSocketChannel serverSocketChannel;
    Selector selector;
    ExecutorService requestHandlerService;
    volatile boolean stop = false;
    volatile boolean opened = false;
    RequestHandler requestHandler;
    
    @Override
    public String toString() {
        return "NetworkServer [socketAddress=" + serverSocketAddress + "]";
    }
    
    public NetworkServer(String hostname, int port) {
        this.serverSocketAddress = new InetSocketAddress(hostname, port);
    }
    
    public void open() {
        try {
//        	System.err.println("Opening selector");
            selector = Selector.open();
//        	System.err.println("Opening server socket");
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
//        	System.err.println("Binding to " + serverSocketAddress);
            serverSocketChannel.socket().bind(serverSocketAddress);
//        	System.err.println("Registering " + serverSocketChannel + " for accepting connections");
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        } catch (IOException e) {
        	NIOUtil.close(selector);
			selector = null;
        	NIOUtil.close(serverSocketChannel);
        	serverSocketChannel = null;
            throw new NetworkException(e);
        }
        requestHandlerService = Executors.newFixedThreadPool(1);
        opened = true;
    }
    
    public void close() {
    	if (opened) {
    		opened = false;
    	}
    	else {
    		return;
    	}
    	System.err.println("Closing server " + serverSocketAddress);
    	stop = true;
    	selector.wakeup();
    	requestHandlerService.shutdown();
    	for (SelectionKey key: selector.keys()) {
    		if (key.isValid() && key.attachment() != null) {
    			key.cancel();
    			NIOUtil.close(key.channel());
    		}
    	}
    	NIOUtil.close(selector);
    	NIOUtil.close(serverSocketChannel);
    }
    
    public void select() {
    	if (!opened || stop) {
    		return;
    	}
        try {
        	for (SelectionKey key: selector.keys()) {
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
        		}
        		else {
        			key.interestOps(SelectionKey.OP_READ);
        		}
        	}
//        	System.err.println("Selecting events");
            selector.select();
            Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
            while (iter.hasNext()) {
                SelectionKey key = iter.next();
                iter.remove();
                
                if (!key.isValid()) {
                    continue;
                }
                if (key.isAcceptable()) {
                    handleAccept(key);
                }
                else if (key.isReadable()) {
                    handleRead(key);
                }
                else if (key.isWritable()) {
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
        RequestDispatcher requestHandler = new RequestDispatcher(protocolHandler, requestHeader, request, responseHeader);
//    	System.err.println("Scheduling request handler for channel " + protocolHandler.socketChannel);
        requestHandlerService.submit(requestHandler);
    }
    
    public void requestStop() {
    	stop = true;
    	selector.wakeup();
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
        final NetworkServer networkServer;
        final SocketChannel socketChannel;

        ByteBuffer readHeader = RequestHeader.allocate();
        RequestHeader requestHeader = new RequestHeader();
        ByteBuffer readPayload = null;
        int readState = READSTATE_INIT;
        static final int READSTATE_INIT = 0;
        static final int READSTATE_HEADER = 1;
        static final int READSTATE_HEADER_COMPLETED = 2;
        static final int READSTATE_PAYLOAD = 3;
        static final int READSTATE_PAYLOAD_COMPLETED = 3;

        int writeState = WRITESTATE_INIT;
        static final int WRITESTATE_INIT = 0;
        static final int WRITESTATE_HEADER = 1;
        static final int WRITESTATE_PAYLOAD = 3;
        static final int WRITESTATE_PAYLOAD_COMPLETED = 3;
        ByteBuffer writeHeader = ResponseHeader.allocate();
        
        ArrayList<WriteRequest> writeQueue = new ArrayList<WriteRequest>();
        WriteRequest current = null;
        
        boolean okay = true;
        
        ProtocolHandler(NetworkServer networkServer, SocketChannel socketChannel) {
            this.networkServer = networkServer;
            this.socketChannel = socketChannel;
        }

        synchronized void doRead(SelectionKey key) {

        	if (!okay) {
        		throw new IllegalStateException("Channel is in error");
        	}
//        	System.err.println("Performing read on channel " + socketChannel);
            try {
                while (true) {
                    if (readState == READSTATE_INIT) {
//                    	System.err.println("READSTATE_INIT");
                        readHeader.clear();
                        requestHeader = new RequestHeader();
                        int n = socketChannel.read(readHeader);
                        if (n == -1) {
                        	failed();
                        	break;
                        }
                        if (readHeader.remaining() == 0) {
                            readState = READSTATE_HEADER_COMPLETED;
                        } else {
                            readState = READSTATE_HEADER;
                            break;
                        }
                    }

                    if (readState == READSTATE_HEADER) {
//                    	System.err.println("READSTATE_PREFIX");
                        int n = socketChannel.read(readHeader);
                        if (n == -1) {
                        	failed();
                        	break;
                        }
                        if (readHeader.remaining() == 0) {
                            readState = READSTATE_HEADER_COMPLETED;
                        } else {
                            break;
                        }
                    }

                    if (readState == READSTATE_HEADER_COMPLETED) {
//                    	System.err.println("READSTATE_PREFIX_COMPLETED");
                        readHeader.rewind();
                        requestHeader.retrieve(readHeader);
                        readPayload = ByteBuffer.allocate(requestHeader.getDataSize());
                        readState = READSTATE_PAYLOAD;
                    }

                    if (readState == READSTATE_PAYLOAD) {
//                    	System.err.println("READSTATE_PAYLOAD");
                        int n = socketChannel.read(readPayload);
                        if (n == -1) {
                        	failed();
                        	break;
                        }
                        if (readPayload.remaining() == 0) {
                            readState = READSTATE_PAYLOAD_COMPLETED;
                        } else {
                            break;
                        }
                    }

                    if (readState == READSTATE_PAYLOAD_COMPLETED) {
//                    	System.err.println("READSTATE_PAYLOAD_COMPLETED");
                        networkServer.queueRequest(this, requestHeader, readPayload);
                        readState = READSTATE_INIT;
                        readPayload = null;
                    }
                }
            } catch (IOException e) {
            	e.printStackTrace();
            	failed();
            }
//        	System.err.println("Finished reading from channel " + socketChannel);
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
//        	System.err.println("Performing write on channel " + socketChannel);
			try {
				while (true) {
					if (current == null) {
						if (writeQueue.size() > 0) {
							current = writeQueue.remove(0);
						}
						else {
							break;
						}
					}
					if (writeState == WRITESTATE_INIT) {
//						System.err.println("WRITESTATE_INIT");
						writeHeader.clear();
						current.getResponseHeader().store(writeHeader);
						writeHeader.flip();
						socketChannel.write(writeHeader);
						if (writeHeader.remaining() == 0) {
							writeState = WRITESTATE_PAYLOAD;
						} else {
							writeState = WRITESTATE_HEADER;
							break;
						}
					}

					if (writeState == WRITESTATE_HEADER) {
//						System.err.println("WRITESTATE_PREFIX");
						socketChannel.write(writeHeader);
						if (writeHeader.remaining() == 0) {
							writeState = WRITESTATE_PAYLOAD;
						} else {
							break;
						}
					}

					if (writeState == WRITESTATE_PAYLOAD) {
//						System.err.println("WRITESTATE_PAYLOAD");
						socketChannel.write(current.getResponse());
						if (current.getResponse().remaining() == 0) {
							writeState = WRITESTATE_PAYLOAD_COMPLETED;
						} else {
							break;
						}
					}

					if (writeState == WRITESTATE_PAYLOAD_COMPLETED) {
//						System.err.println("WRITESTATE_PAYLOAD_COMPLETED");
						writeState = WRITESTATE_INIT;
						current = null;
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
				failed();
			}
//        	System.err.println("Finished writing to channel " + socketChannel);
		}
        
        synchronized void queueWrite(WriteRequest wr) {
        	wr.responseHeader.setDataSize(wr.response.limit());
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
        
        final RequestHandler requestHandler = new EchoRequestHandler();
        
        RequestDispatcher(ProtocolHandler protocolHandler, RequestHeader requestHeader, ByteBuffer request, ResponseHeader responseHeader) {
            this.protocolHandler = protocolHandler;
            this.requestHeader = requestHeader;
            this.responseHeader = responseHeader;
            this.data = request;
        }

        public Object call() throws Exception {
            data.rewind();
            Request request = new RequestImpl(requestHeader, data);
            Response response = new ResponseImpl(responseHeader, null);
            requestHandler.handleRequest(request, response);
            // TODO support the no reply option
            protocolHandler.queueWrite(new WriteRequest(responseHeader, response.getData()));
            return null;
        }       
    }
}

