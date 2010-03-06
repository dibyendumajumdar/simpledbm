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

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.simpledbm.common.api.exception.SimpleDBMException;
import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.util.Dumpable;
import org.simpledbm.common.util.TypeSize;
import org.simpledbm.common.util.mcat.Message;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.common.util.mcat.MessageType;
import org.simpledbm.database.api.Database;
import org.simpledbm.database.api.DatabaseFactory;
import org.simpledbm.database.api.Table;
import org.simpledbm.database.api.TableScan;
import org.simpledbm.network.common.api.AddRowMessage;
import org.simpledbm.network.common.api.CloseScanMessage;
import org.simpledbm.network.common.api.DeleteRowMessage;
import org.simpledbm.network.common.api.EndTransactionMessage;
import org.simpledbm.network.common.api.FetchNextRowMessage;
import org.simpledbm.network.common.api.FetchNextRowReply;
import org.simpledbm.network.common.api.GetTableMessage;
import org.simpledbm.network.common.api.OpenScanMessage;
import org.simpledbm.network.common.api.QueryDictionaryMessage;
import org.simpledbm.network.common.api.RequestCode;
import org.simpledbm.network.common.api.StartTransactionMessage;
import org.simpledbm.network.common.api.UpdateRowMessage;
import org.simpledbm.network.nio.api.Request;
import org.simpledbm.network.nio.api.RequestHandler;
import org.simpledbm.network.nio.api.Response;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.typesystem.api.TableDefinition;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.api.TypeFactory;

public class SimpleDBMRequestHandler implements RequestHandler {

	Database database;
	Platform platform;
	PlatformObjects po;

	/**
	 * A map of all active sessions
	 */
	HashMap<Integer, ClientSession> sessions = new HashMap<Integer, ClientSession>();

	/**
	 * A sequence number generator used to allocate new session ids.
	 */
	AtomicInteger sessionIdGenerator = new AtomicInteger(0);

	/*
	 * Messages
	 */
	static final Message UnexpectedError = new Message('N', 'S', MessageType.ERROR, 1, "Unexpected error while converting message to UTF-8 format");
	static final Message noSuchScan = new Message('N', 'S', MessageType.ERROR, 2, "Table Scan {0} does not exist");
	
	private ClientSession validateSession(Request request, Response response) {
		ClientSession session = null;
		synchronized(sessions) {
			session = sessions.get(request.getSessionId());
			if (session == null) {
				setError(response, -1, "Unknown session identifier");
			}
		}
		return session;
	}

	public void handleRequest(Request request, Response response) {
		if (request.getRequestCode() == RequestCode.OPEN_SESSION) {
			handleOpenSessionRequest(request, response);
		} else if (request.getRequestCode() == RequestCode.CLOSE_SESSION) {
			handleCloseSessionRequest(request, response);
		} else if (request.getRequestCode() == RequestCode.QUERY_DICTIONARY) {
			handleQueryDictionaryRequest(request, response);
		} else if (request.getRequestCode() == RequestCode.CREATE_TABLE) {
			handleCreateTable(request, response);
		} else if (request.getRequestCode() == RequestCode.START_TRANSACTION) {
			handleStartTransaction(request, response);
		} else if (request.getRequestCode() == RequestCode.END_TRANSACTION) {
			handleEndTransaction(request, response);
		} else if (request.getRequestCode() == RequestCode.GET_TABLE) {
			handleGetTable(request, response);
		} else if (request.getRequestCode() == RequestCode.OPEN_TABLESCAN) {
			handleOpenTableScan(request, response);
		} else if (request.getRequestCode() == RequestCode.CLOSE_TABLESCAN) {
			handleCloseTableScan(request, response);
		} else if (request.getRequestCode() == RequestCode.ADD_ROW) {
			handleAddRow(request, response);
		} else if (request.getRequestCode() == RequestCode.FETCH_NEXT_ROW) {
			handleFetchNextRow(request, response);
		} else if (request.getRequestCode() == RequestCode.UPDATE_CURRENT_ROW) {
			handleUpdateCurrentRow(request, response);
		} else if (request.getRequestCode() == RequestCode.DELETE_CURRENT_ROW) {
			handleDeleteCurrentRow(request, response);
		} else {
			handleUnknownRequest(request, response);
		}
	}

	public void onInitialize(Platform platform, Properties properties) {
		this.platform = platform;
		this.po = platform.getPlatformObjects(SimpleDBMServer.LOGGER_NAME);
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
		byte[] bytes;
		try {
			bytes = message.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new SimpleDBMException(new MessageInstance(UnexpectedError), e);
		}
		ByteBuffer data = ByteBuffer.wrap(bytes);
		data.limit(bytes.length);
		response.setData(data);
	}

	private void formatException(StringBuilder sb, Throwable e) {
		sb.append(e.getClass().getName());
		sb.append(": ");
		sb.append(e.getMessage());
		sb.append(Dumpable.newline);
		for (StackTraceElement se : e.getStackTrace()) {
			sb.append(Dumpable.TAB);
			sb.append("at ");
			sb.append(se.toString());
			sb.append(Dumpable.newline);
		}		
	}
	
	private void setError(Response response, int statusCode, String message,
			Throwable e) {
		response.setStatusCode(statusCode);
		StringBuilder sb = new StringBuilder();
		sb.append(message);
		sb.append(Dumpable.newline);
		do {
			formatException(sb, e);
			e = e.getCause();
			if (e != null) {
				sb.append("Caused by: ");
			}
		} while (e != null);
		byte[] bytes;
		try {
			bytes = sb.toString().getBytes("UTF-8");
		} catch (UnsupportedEncodingException e1) {
			throw new SimpleDBMException(new MessageInstance(UnexpectedError), e1);
		}
		ByteBuffer data = ByteBuffer.wrap(bytes);
		data.limit(bytes.length);
		response.setData(data);
	}
	
	void handleOpenSessionRequest(Request request, Response response) {
		int sessionId = sessionIdGenerator.incrementAndGet();
		ClientSession session = new ClientSession(sessionId, database);
		synchronized (session) {
			sessions.put(sessionId, session);
		}
		response.setSessionId(sessionId);
	}

	void handleCloseSessionRequest(Request request, Response response) {
		int sessionId = request.getSessionId();
//		System.err.println("Request to close session " + sessionId);
		synchronized (sessions) {
			ClientSession session = sessions.get(sessionId);
			if (session == null) {
				setError(response, -1, "Unknown session identifier: " + sessionId);
			} else {
//				System.err.println("session removed");
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
			setError(response, -1, "Failed to query data dictionary", e);
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
			setError(response, -1, "Failed to create test tables", e);
		}
	}
	
	void handleCreateTable(Request request, Response response) {
		ClientSession session = validateSession(request, response);
		if (session == null) {
			return;
		}
		try {
			TableDefinition tableDefinition = database.getTypeSystemFactory().getTableDefinition(
					database.getPlatformObjects(), database.getTypeFactory(),
					database.getRowFactory(), request.getData());
			database.createTable(tableDefinition);
		}
		catch (Exception e) {
			setError(response, -1, "Failed to create table", e);
		}
	}

	void handleStartTransaction(Request request, Response response) {
		ClientSession session = validateSession(request, response);
		if (session == null) {
			return;
		}
		Transaction transaction = session.getTransaction();
		if (transaction != null) {
			setError(response, -1, "Has an active transaction: " + transaction);
			return;
		}
		StartTransactionMessage message = new StartTransactionMessage(request.getData());
		try {
			transaction = database.startTransaction(message.getIsolationMode());
			session.setTransaction(transaction);
		}
		catch (Exception e) {
			setError(response, -1, "Failed to start transaction", e);
		}
	}

	void handleEndTransaction(Request request, Response response) {
		ClientSession session = validateSession(request, response);
		if (session == null) {
			return;
		}
		Transaction transaction = session.getTransaction();
		if (transaction == null) {
			setError(response, -1, "There is no active transaction");
			return;
		}
		EndTransactionMessage message = new EndTransactionMessage(request.getData());
		try {
			if (message.isCommit()) {
				transaction.commit();
			}
			else {
				transaction.abort();
			}
			session.setTransaction(null);
		}
		catch (Exception e) {
			setError(response, -1, "Failed to end transaction", e);
		}
	}
	
	void handleGetTable(Request request, Response response) {
		ClientSession session = validateSession(request, response);
		if (session == null) {
			return;
		}
		Transaction transaction = session.getTransaction();
		if (transaction == null) {
			setError(response, -1, "There is no active transaction");
			return;
		}
		GetTableMessage message = new GetTableMessage(request.getData());
		try {
			Table table = session.getTable(message.containerId);
			TableDefinition tableDefinition = table.getDefinition();
			ByteBuffer bb = ByteBuffer.allocate(tableDefinition.getStoredLength());
			tableDefinition.store(bb);
			bb.flip();
			response.setData(bb);
		}
		catch (Exception e) {
			setError(response, -1, "Failed to get table " + message.containerId, e);
		}
	}
	
	void handleOpenTableScan(Request request, Response response) {
		ClientSession session = validateSession(request, response);
		if (session == null) {
			return;
		}
		Transaction transaction = session.getTransaction();
		if (transaction == null) {
			setError(response, -1, "There is no active transaction");
			return;
		}
		OpenScanMessage message = new OpenScanMessage(database.getRowFactory(), request.getData());
		try {
			Table table = session.getTable(message.getContainerId());
			if (table == null) {
				throw new RuntimeException("No such table");
			}
			TableScan tableScan = table.openScan(transaction, message.getIndexNo(), 
					message.getStartRow(), message.isForUpdate());
			int scanId = session.registerTableScan(tableScan);
			ByteBuffer bb = ByteBuffer.allocate(TypeSize.INTEGER);
			bb.putInt(scanId);
			bb.flip();
			response.setData(bb);
		}
		catch (Exception e) {
			setError(response, -1, "Failed to open scan for table " + message.getContainerId(), e);
		}
	}

	void handleCloseTableScan(Request request, Response response) {
		ClientSession session = validateSession(request, response);
		if (session == null) {
			return;
		}
		Transaction transaction = session.getTransaction();
		if (transaction == null) {
			setError(response, -1, "There is no active transaction");
			return;
		}
		CloseScanMessage message = new CloseScanMessage(request.getData());
		try {
			TableScan tableScan = session.getTableScan(message.getScanId());
			if (tableScan == null) {
				throw new RuntimeException("No such table scan");
			}
			tableScan.close();
		}
		catch (Exception e) {
			setError(response, -1, "Failed to close scan # " + message.getScanId(), e);
		}
	}
	
	void handleAddRow(Request request, Response response) {
		ClientSession session = validateSession(request, response);
		if (session == null) {
			return;
		}
		Transaction transaction = session.getTransaction();
		if (transaction == null) {
			setError(response, -1, "There is no active transaction");
			return;
		}
		AddRowMessage message = new AddRowMessage(database.getRowFactory(), request.getData());
		try {
			Table table = session.getTable(message.getContainerId());
			if (table == null) {
				throw new RuntimeException("No such table");
			}
//			System.err.println("Adding row " + message.getRow());
			table.addRow(transaction, message.getRow());
		}
		catch (Exception e) {
			setError(response, -1, "Failed to add row to table " + message.getContainerId(), e);
		}
	}	

	void handleFetchNextRow(Request request, Response response) {
		ClientSession session = validateSession(request, response);
		if (session == null) {
			return;
		}
		Transaction transaction = session.getTransaction();
		if (transaction == null) {
			setError(response, -1, "There is no active transaction");
			return;
		}
		FetchNextRowMessage message = new FetchNextRowMessage(database.getRowFactory(), request.getData());
		try {
			TableScan tableScan = session.getTableScan(message.getScanId());
			if (tableScan == null) {
				throw new SimpleDBMException(new MessageInstance(noSuchScan, message.getScanId()));
			}
			FetchNextRowReply reply = null;
			boolean hasNext = tableScan.fetchNext();
			if (hasNext) {
				reply = new FetchNextRowReply(tableScan.getTable().getDefinition().getContainerId(), 
						false, tableScan.getCurrentRow());
			}
			else {
				reply = new FetchNextRowReply(tableScan.getTable().getDefinition().getContainerId(), true, null);
			}
			ByteBuffer bb = ByteBuffer.allocate(reply.getStoredLength());
			reply.store(bb);
			bb.flip();
			response.setData(bb);
		}
		catch (Exception e) {
			setError(response, -1, "Failed to fetch the next row", e);
		}
	}
	
	void handleUpdateCurrentRow(Request request, Response response) {
		ClientSession session = validateSession(request, response);
		if (session == null) {
			return;
		}
		Transaction transaction = session.getTransaction();
		if (transaction == null) {
			setError(response, -1, "There is no active transaction");
			return;
		}
		UpdateRowMessage message = new UpdateRowMessage(database.getRowFactory(), request.getData());
		try {
			TableScan tableScan = session.getTableScan(message.getScanId());
			if (tableScan == null) {
				throw new SimpleDBMException(new MessageInstance(noSuchScan, message.getScanId()));
			}
			tableScan.updateCurrentRow(message.getRow());
		}
		catch (Exception e) {
			setError(response, -1, "Failed to update row", e);
		}
	}

	/**
	 * Process a delete row request
	 */
	void handleDeleteCurrentRow(Request request, Response response) {
		ClientSession session = validateSession(request, response);
		if (session == null) {
			return;
		}
		Transaction transaction = session.getTransaction();
		if (transaction == null) {
			setError(response, -1, "There is no active transaction");
			return;
		}
		DeleteRowMessage message = new DeleteRowMessage(request.getData());
		try {
			TableScan tableScan = session.getTableScan(message.getScanId());
			if (tableScan == null) {
				throw new SimpleDBMException(new MessageInstance(noSuchScan, message.getScanId()));
			}
			tableScan.deleteRow();
		}
		catch (Exception e) {
			setError(response, -1, "Failed to delete row", e);
		}
	}
	
	/*
	 * On shutdown we must abort transactions that weren't committed by
	 * respective clients We should also periodically check on the session
	 * activity and timeout sessions that are inactive for a while.
	 */
}
