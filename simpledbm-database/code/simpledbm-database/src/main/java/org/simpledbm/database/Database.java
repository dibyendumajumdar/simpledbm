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
 *    Project: www.simpledbm.org
 *    Author : Dibyendu Majumdar
 *    Email  : dibyendu@mazumdar.demon.co.uk
 */
package org.simpledbm.database;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Properties;

import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.tx.BaseLoggable;
import org.simpledbm.rss.api.tx.BaseTransactionalModule;
import org.simpledbm.rss.api.tx.IsolationMode;
import org.simpledbm.rss.api.tx.PostCommitAction;
import org.simpledbm.rss.api.tx.Redoable;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.rss.main.Server;
import org.simpledbm.typesystem.api.FieldFactory;
import org.simpledbm.typesystem.api.RowFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.impl.DefaultFieldFactory;
import org.simpledbm.typesystem.impl.GenericRowFactory;

public class Database extends BaseTransactionalModule {

	final static int MODULE_ID = 10;
	final static int MODULE_BASE = 80;

	/** Object registry id for row factory */
	final static int ROW_FACTORY_TYPE_ID = MODULE_BASE+1;
	final static int TYPE_CREATE_TABLE_DEFINITION = MODULE_BASE+2;
	
	Server server;

	Properties properties;
	
	private boolean serverStarted = false;

	final FieldFactory fieldFactory = new DefaultFieldFactory();

	final RowFactory rowFactory = new GenericRowFactory(fieldFactory);

	ArrayList<Table> tables = new ArrayList<Table>();

	public Table addTableDefinition(String name, int containerId,
			TypeDescriptor[] rowType) {
		return new Table(this, containerId, name, rowType);
	}

	public Database(Properties properties) {
		validateProperties(properties);
		this.properties = properties;
	}
	
	private void validateProperties(Properties properties2) {
		// TODO Auto-generated method stub
		
	}

	private void createSystemTables() {
		// TODO
	}
	
	public static void create(Properties properties) {
		Server.create(properties);
		
		Database db = new Database(properties);
		db.start();
		try {
			db.createSystemTables();
		}
		finally {
			db.shutdown();
		}
	}

	public void start() {
        /*
         * We cannot start the server more than once
         */
        if (serverStarted) {
            throw new RuntimeException("Server is already started");
        }

        /*
         * We must always create a new server object.
         */
        server = new Server(properties);
		server.getModuleRegistry().registerModule(MODULE_ID, this);        
		registerRowFactory();
        server.start();

        serverStarted = true;
	}

	private void registerRowFactory() {
        server.registerSingleton(ROW_FACTORY_TYPE_ID, rowFactory);
	}

	public void shutdown() {
        if (serverStarted) {
            server.shutdown();
            serverStarted = false;
            server = null;
        }
	}

	public Server getServer() {
		return server;
	}

	public FieldFactory getFieldFactory() {
		return fieldFactory;
	}

	public RowFactory getRowFactory() {
		return rowFactory;
	}

	public void createTable(Table tableDefinition) {
		Transaction trx = server.begin(IsolationMode.READ_COMMITTED);
		boolean success = false;
		try {
			server.createTupleContainer(trx, tableDefinition.getName(),
					tableDefinition.getContainerId(), 8);
			for (Index idx : tableDefinition.getIndexes()) {
				server.createIndex(trx, idx.getName(), idx.getContainerId(), 8,
						ROW_FACTORY_TYPE_ID, idx.isUnique());
			}
			trx.schedulePostCommitAction(new CreateTableDefinition());
			success = true;
		} finally {
			if (success)
				trx.commit();
			else
				trx.abort();
		}
	}

	@Override
	public void redo(Page page, Redoable loggable) {
		if (loggable instanceof CreateTableDefinition) {
			// TODO
		}
		
	}

	/**
	 * Responsible for adding the table definition to the data dictionary.
	 * 
	 * @author dibyendumajumdar
	 * @since 29 Dec 2007
	 */
	public static class CreateTableDefinition extends BaseLoggable implements PostCommitAction {

		@Override
		public void init() {
			// TODO Auto-generated method stub
			
		}

		public int getActionId() {
			// TODO Auto-generated method stub
			return 0;
		}

		public void setActionId(int actionId) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public StringBuilder appendTo(StringBuilder sb) {
			// TODO Auto-generated method stub
			return super.appendTo(sb);
		}

		@Override
		public int getStoredLength() {
			// TODO Auto-generated method stub
			return super.getStoredLength();
		}

		@Override
		public void retrieve(ByteBuffer bb) {
			// TODO Auto-generated method stub
			super.retrieve(bb);
		}

		@Override
		public void store(ByteBuffer bb) {
			// TODO Auto-generated method stub
			super.store(bb);
		}

		@Override
		public String toString() {
			// TODO Auto-generated method stub
			return super.toString();
		}
		
	}
	
}
