package org.simpledbm.samples.tupledemo;

import java.util.Properties;

import org.simpledbm.rss.api.im.Index;
import org.simpledbm.rss.api.im.IndexKeyFactory;
import org.simpledbm.rss.api.tuple.TupleContainer;
import org.simpledbm.rss.api.tuple.TupleInserter;
import org.simpledbm.rss.api.tx.IsolationMode;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.rss.main.Server;
import org.simpledbm.typesystem.api.Field;
import org.simpledbm.typesystem.api.FieldFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.impl.DefaultFieldFactory;
import org.simpledbm.typesystem.impl.GenericIndexKeyFactory;
import org.simpledbm.typesystem.impl.IndexRow;
import org.simpledbm.typesystem.impl.IntegerType;
import org.simpledbm.typesystem.impl.VarcharType;

public class TupleDemo1 {

	static class TupleDemoDb {
		
		private Server server;

		private boolean serverStarted = false;
		
		final static int TABLE_CONTNO = 1;
		final static int PKEY_CONTNO = 2;
		final static int SKEY1_CONTNO = 3;

		final static int ROW_FACTORY_TYPE_ID = 25000;
		
		/**
		 * Creates the SimpleDBM server. 
		 */
		public static void createServer() {
    		Properties properties = new Properties();
    		properties.setProperty("log.ctl.1", "ctl.a");
    		properties.setProperty("log.ctl.2", "ctl.b");
    		properties.setProperty("log.groups.1.path", ".");
    		properties.setProperty("log.archive.path", ".");
    		properties.setProperty("log.group.files", "3");
    		properties.setProperty("log.file.size", "16384");
    		properties.setProperty("log.buffer.size", "16384");
    		properties.setProperty("log.buffer.limit", "4");
    		properties.setProperty("log.flush.interval", "5");
    		properties.setProperty("storage.basePath", "demodata/TupleDemo1");

   			Server.create(properties);
		}
		
		/**
		 * Starts the SimpleDBM server instance.
		 */
		public void startServer() {

			/*
			 * We cannot start the server more than once
			 */
			if (serverStarted) {
				throw new RuntimeException("Server is already started");
			}
			
    		Properties properties = new Properties();
    		properties.setProperty("log.ctl.1", "ctl.a");
    		properties.setProperty("log.ctl.2", "ctl.b");
    		properties.setProperty("log.groups.1.path", ".");
    		properties.setProperty("log.archive.path", ".");
    		properties.setProperty("log.group.files", "3");
    		properties.setProperty("log.file.size", "16384");
    		properties.setProperty("log.buffer.size", "16384");
    		properties.setProperty("log.buffer.limit", "4");
    		properties.setProperty("log.flush.interval", "5");
    		properties.setProperty("storage.basePath", "demodata/TupleDemo1");

    		/*
    		 * We must always create a new server object. 
    		 */
    		server = new Server(properties);
    		server.start();
    		
    		serverStarted = true;
		}
		
		/**
		 * Shuts down the SimpleDBM server instance.
		 */
		public void shutdownServer() {
			if (serverStarted) {
				server.shutdown();
				serverStarted = false;
				server = null;
			}
		}
		
		/**
		 * Registers a row types for the table, primary key index, and secondary key index.
		 */
		public void registerTableRowType() {

			final FieldFactory fieldFactory = new DefaultFieldFactory();

			final GenericIndexKeyFactory keyFactory = new GenericIndexKeyFactory(fieldFactory);

			/**
			 * Table row (id, name, surname, city)
			 */
			final TypeDescriptor[] rowtype_for_mytable = new TypeDescriptor[] { 
					new IntegerType(),	/* primary key */
					new VarcharType(30), /* name */
					new VarcharType(30), /* surname */
					new VarcharType(20) /* city */
					};

			/**
			 * Primary key (id)
			 */
			final TypeDescriptor[] rowtype_for_pk = new TypeDescriptor[] { 
					rowtype_for_mytable[0]	/* primary key */
					};
			
			/**
			 * Secondary key (name, surname)
			 */
			final TypeDescriptor[] rowtype_for_sk1 = new TypeDescriptor[] { 
					rowtype_for_mytable[1], /* name */
					rowtype_for_mytable[2], /* surname */
					};
			
			keyFactory.registerRowType(TABLE_CONTNO, rowtype_for_mytable);
			keyFactory.registerRowType(PKEY_CONTNO, rowtype_for_pk);
			keyFactory.registerRowType(SKEY1_CONTNO, rowtype_for_sk1);
			
			server.getObjectRegistry().register(ROW_FACTORY_TYPE_ID, keyFactory);
		}

		/**
		 * Creates a new row object for the specified container.
		 * @param containerId ID of the container
		 * @return Appropriate row type
		 */
		IndexRow makeRow(int containerId) {
			IndexKeyFactory keyFactory = (IndexKeyFactory) server.getObjectRegistry().getInstance(ROW_FACTORY_TYPE_ID);
			return (IndexRow) keyFactory.newIndexKey(containerId);
		}
		
		/**
		 * Creates the table and associated indexes
		 */
		public void createTableAndIndexes() {

			Transaction trx = server.getTransactionManager().begin(IsolationMode.CURSOR_STABILITY);
			boolean success = false;
			try {
				server.getTupleManager().createTupleContainer(trx, "MYTABLE.DAT", TABLE_CONTNO, 8);
				success = true;
			} finally {
				if (success)
					trx.commit();
				else
					trx.abort();
			}
			
			trx = server.getTransactionManager().begin(IsolationMode.CURSOR_STABILITY);
			success = false;
			try {
				server.getIndexManager().createIndex(trx, "MYTABLE_PK.IDX", PKEY_CONTNO, 8, ROW_FACTORY_TYPE_ID, server.getTupleManager().getLocationFactoryType(), true);
				success = true;
			} finally {
				if (success)
					trx.commit();
				else
					trx.abort();
			}

			trx = server.getTransactionManager().begin(IsolationMode.CURSOR_STABILITY);
			success = false;
			try {
				server.getIndexManager().createIndex(trx, "MYTABLE_SKEY1.IDX", SKEY1_CONTNO, 8, ROW_FACTORY_TYPE_ID, server.getTupleManager().getLocationFactoryType(), false);
				success = true;
			} finally {
				if (success)
					trx.commit();
				else
					trx.abort();
			}
		}
		
		/**
		 * Adds a new row to the table, and updates associated indexes.
		 * @param tableRow Row to be added to the table
		 * @throws CloneNotSupportedException 
		 */
		public void addRow(int id, String name, String surname, String city) throws CloneNotSupportedException {
			
			TupleContainer table = server.getTupleManager().getTupleContainer(TABLE_CONTNO);
			Index primaryIndex = server.getIndexManager().getIndex(PKEY_CONTNO);
			Index secondaryIndex = server.getIndexManager().getIndex(SKEY1_CONTNO);
			
			IndexRow tableRow = makeRow(TABLE_CONTNO);
			tableRow.get(0).setInt(id);
			tableRow.get(1).setString(name);
			tableRow.get(2).setString(surname);		
			tableRow.get(3).setString(city);
			
			Transaction trx = server.getTransactionManager().begin(IsolationMode.CURSOR_STABILITY);
			boolean success = false;
			try {
				IndexRow primaryKeyRow = makeRow(PKEY_CONTNO);
				// Set id
				primaryKeyRow.set(0, (Field) tableRow.get(0).clone());
			
				IndexRow secondaryKeyRow = makeRow(SKEY1_CONTNO);
				// Set name
				secondaryKeyRow.set(0, (Field) tableRow.get(1).clone());
				// Set surname
				secondaryKeyRow.set(1, (Field) tableRow.get(2).clone());
				
				TupleInserter inserter = table.insert(trx, tableRow);
				primaryIndex.insert(trx, primaryKeyRow, inserter.getLocation());
				secondaryIndex.insert(trx, secondaryKeyRow, inserter.getLocation());
				inserter.completeInsert();
			}
			finally {
				if (success) {
					trx.commit();
				}
				else {
					trx.abort();
				}
			}
		}
		
		public void listRowsByKey(int key) {
			
		}
		
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
