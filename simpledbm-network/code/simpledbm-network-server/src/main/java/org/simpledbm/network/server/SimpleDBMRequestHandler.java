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
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.database.api.Database;
import org.simpledbm.database.api.DatabaseFactory;
import org.simpledbm.database.api.TableDefinition;
import org.simpledbm.database.impl.TableDefinitionImpl;
import org.simpledbm.network.common.api.QueryDictionaryMessage;
import org.simpledbm.network.common.api.RequestCode;
import org.simpledbm.network.nio.api.Request;
import org.simpledbm.network.nio.api.RequestHandler;
import org.simpledbm.network.nio.api.Response;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.api.TypeFactory;

public class SimpleDBMRequestHandler implements RequestHandler {

	Database database;
	Platform platform;

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
		} else if (request.getRequestCode() == RequestCode.CREATE_TEST_TABLES) {
			handleCreateTestTables(request, response);
		} else if (request.getRequestCode() == RequestCode.QUERY_DICTIONARY) {
			handleQueryDictionaryRequest(request, response);
		} else if (request.getRequestCode() == RequestCode.CREATE_TABLE) {
			handleCreateTable(request, response);
		} else {
			handleUnknownRequest(request, response);
		}
	}

	public void onInitialize(Platform platform, Properties properties) {
		this.platform = platform;
		database = DatabaseFactory.getDatabase(platform, properties);
	}

	public void onShutdown() {
		database.shutdown();
	}

	public void onStart() {
		database.start();
	}

	private void setError(Response response, int statusCode, String message) {
		response.setStatusCode(statusCode);
		byte[] bytes = message.getBytes(Charset.forName("UTF-8"));
		ByteBuffer data = ByteBuffer.wrap(bytes);
		data.limit(bytes.length);
		response.setData(data);
	}

	void handleOpenSessionRequest(Request request, Response response) {
		int sessionId = sessionIdGenerator.incrementAndGet();
		ClientSession session = new ClientSession(sessionId);
		synchronized (session) {
			sessions.put(sessionId, session);
		}
		response.setSessionId(sessionId);
	}

	void handleCloseSessionRequest(Request request, Response response) {
		int sessionId = request.getSessionId();
		System.err.println("Request to close session " + sessionId);
		synchronized (sessions) {
			ClientSession session = sessions.get(sessionId);
			if (session == null) {
				setError(response, -1, "Unknown session identifier");
			} else {
				System.err.println("session removed");
				sessions.remove(sessionId);
			}
		}
		response.setSessionId(0);
	}
	
	void handleQueryDictionaryRequest(Request request, Response response) {
		try {
			QueryDictionaryMessage message = new QueryDictionaryMessage(request
					.getData());
			TypeDescriptor[] td = database.getDictionaryCache()
					.getTypeDescriptor(message.containerId);
			ByteBuffer bb = ByteBuffer.allocate(database.getTypeFactory()
					.getStoredLength(td));
			database.getTypeFactory().store(td, bb);
			bb.flip();
			response.setData(bb);
		} catch (Exception e) {
			e.printStackTrace();
			setError(response, -1, "Failed to query data dictionary: "
					+ e.getMessage());
		}
	}

	void handleUnknownRequest(Request request, Response response) {
		int sessionId = request.getSessionId();
		setError(response, -1, "Received invalid request "
				+ request.getRequestCode() + " from " + sessionId);
		response.setSessionId(0);
	}

	void handleCreateTestTables(Request request, Response response) {
		try {
			TypeFactory ff = database.getTypeFactory();
			TypeDescriptor employee_rowtype[] = { 
					ff.getIntegerType(), /* primary key */
					ff.getVarcharType(20), /* name */
					ff.getVarcharType(20), /* surname */
					ff.getVarcharType(20), /* city */
					ff.getVarcharType(45), /* email address */
					ff.getDateTimeType(), /* date of birth */
					ff.getNumberType(2) /* salary */
			};
			TableDefinition tableDefinition = database.newTableDefinition(
					"employee", 1, employee_rowtype);
			tableDefinition.addIndex(2, "employee1.idx", new int[] { 0 }, true,
					true);
			tableDefinition.addIndex(3, "employee2.idx", new int[] { 2, 1 },
					false, false);
			tableDefinition.addIndex(4, "employee3.idx", new int[] { 5 },
					false, false);
			tableDefinition.addIndex(5, "employee4.idx", new int[] { 6 },
					false, false);

			database.createTable(tableDefinition);
		} catch (Exception e) {
			e.printStackTrace();
			setError(response, -1, "Failed to create test tables: "
					+ e.getMessage());
		}
	}
	
	void handleCreateTable(Request request, Response response) {
		try {
			TableDefinition tableDefinition = new TableDefinitionImpl(
					database.getPlatformObjects(), database.getTypeFactory(),
					database.getRowFactory(), request.getData());
			database.createTable(tableDefinition);
		}
		catch (Exception e) {
			e.printStackTrace();
			setError(response, -1, "Failed to create table: "
					+ e.getMessage());
		}
	}
	
	/*
	 * On shutdown we must abort transactions that weren't committed by
	 * respective clients We should also periodically check on the session
	 * activity and timeout sessions that are inactive for a while.
	 */
}
