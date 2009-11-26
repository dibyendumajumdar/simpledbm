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
package org.simpledbm.network.server;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.database.api.Database;
import org.simpledbm.database.api.DatabaseFactory;
import org.simpledbm.network.common.api.RequestCode;
import org.simpledbm.network.nio.api.Request;
import org.simpledbm.network.nio.api.RequestHandler;
import org.simpledbm.network.nio.api.Response;

public class SimpleDBMRequestHandler implements RequestHandler {

	Database database;

	/**
	 * A map of all active sessions
	 */
	HashMap<Integer, ClientSession> sessions = new HashMap<Integer, ClientSession>();

	/**
	 * A sequence number generator used to allocate new session ids.
	 */
	AtomicInteger sessionIdGenerator = new AtomicInteger(0);

	public void handleRequest(Request request, Response response) {
		if (request.getRequestCode() == RequestCode.OPEN_SESSION) {
			handleOpenSessionRequest(request, response);
		} else if (request.getRequestCode() == RequestCode.CLOSE_SESSION) {
			handleCloseSessionRequest(request, response);
		} else {
			handleUnknownRequest(request, response);
		}
	}

	public void onInitialize(Platform platform, Properties properties) {
		database = DatabaseFactory.getDatabase(platform, properties);
	}

	public void onShutdown() {
		database.shutdown();
	}

	public void onStart() {
		database.start();
	}

	void handleOpenSessionRequest(Request request, Response response) {
		int sessionId = sessionIdGenerator.incrementAndGet();
		ClientSession session = new ClientSession(sessionId);
		sessions.put(sessionId, session);
		response.setSessionId(sessionId);
		response.setStatusCode(0);
	}

	void handleCloseSessionRequest(Request request, Response response) {
		int sessionId = request.getSessionId();
		System.err.println("Request to close session " + sessionId);
		ClientSession session = sessions.get(sessionId);
		if (session == null) {
			response.setStatusCode(-1);
			response.setData(ByteBuffer.wrap("Unknown session identifier"
					.getBytes()));
			response.setSessionId(0);
		} else {
			System.err.println("session removed");
			sessions.remove(sessionId);
			response.setStatusCode(0);
			response.setData(ByteBuffer.allocate(0));
			response.setSessionId(0);
		}
	}

	void handleUnknownRequest(Request request, Response response) {
		int sessionId = request.getSessionId();
		System.err.println("Received invalid request "
				+ request.getRequestCode() + " from " + sessionId);
		response.setStatusCode(-1);
		response.setData(ByteBuffer.wrap("Unknown request code"
				.getBytes()));
		response.setSessionId(0);
	}
	
	/*
	 * On shutdown we must abort transactions that weren't committed by respective clients
	 * We should also periodically check on the session activity and timeout sessions that 
	 * are inactive for a while.
	 */

}