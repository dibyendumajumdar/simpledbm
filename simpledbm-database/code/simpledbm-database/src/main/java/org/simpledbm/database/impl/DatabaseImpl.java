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
package org.simpledbm.database.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;

import org.simpledbm.database.api.Database;
import org.simpledbm.database.api.IndexDefinition;
import org.simpledbm.database.api.Table;
import org.simpledbm.database.api.TableDefinition;
import org.simpledbm.exception.DatabaseException;
import org.simpledbm.rss.api.locking.LockMode;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.registry.ObjectRegistry;
import org.simpledbm.rss.api.registry.ObjectRegistryAware;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.st.StorageContainerInfo;
import org.simpledbm.rss.api.tx.BaseLoggable;
import org.simpledbm.rss.api.tx.BaseTransactionalModule;
import org.simpledbm.rss.api.tx.IsolationMode;
import org.simpledbm.rss.api.tx.Loggable;
import org.simpledbm.rss.api.tx.PostCommitAction;
import org.simpledbm.rss.api.tx.Redoable;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.rss.main.Server;
import org.simpledbm.rss.util.TypeSize;
import org.simpledbm.rss.util.logging.Logger;
import org.simpledbm.rss.util.mcat.MessageCatalog;
import org.simpledbm.typesystem.api.FieldFactory;
import org.simpledbm.typesystem.api.RowFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.impl.DefaultFieldFactory;

/**
 * The Database Manager is implemented as a Transactional Module because it
 * manages the persistence of the data dictionary.
 * 
 * @author dibyendumajumdar
 */
public class DatabaseImpl extends BaseTransactionalModule implements Database {

	/*
	 * Data Dictionary implementation notes: The Data Dictionary is not
	 * maintained in tables. This is to avoid the chicken and egg situation that
	 * would occur. Instead, the table definitions are stored in a custom
	 * serialized format. Each table definition (along with associated index
	 * definitions) is stored in a dedicated container. The system maintains all
	 * the definitions in memory as well. At startup, all existing definitions
	 * are loaded by the system into memory. New definitions are added to the
	 * memory cache as well as persisted to the database as described above. At
	 * present there is no support for deleting a table. Also indexes cannot be
	 * added at a later stage.
	 */

	/* ID for this module */
	final static int MODULE_ID = 100;
	final static int MODULE_BASE = 101;
	/** Object registry id for row factory */
	final static int ROW_FACTORY_TYPE_ID = MODULE_BASE + 1;
	/** Type ID for Loggable object */
	final static int TYPE_CREATE_TABLE_DEFINITION = MODULE_BASE + 2;
	/** RSS Server object */
	Server server;
	/** Database startup/create properties */
	Properties properties;
	/** Flag to indicate whether the database has been started */
	private boolean serverStarted = false;
	/** The TypeSystem Factory we will use for costructing types */
	final FieldFactory fieldFactory = new DefaultFieldFactory();
	/** The RowFactory we will use for constructing rows */
	final RowFactory rowFactory = new DatabaseRowFactoryImpl(this, fieldFactory);

	static Logger log = Logger.getLogger(DatabaseImpl.class.getPackage()
			.getName());

	static {
		/*
		 * Add messages to the Message Catalog.
		 */
		MessageCatalog.addMessage("WD0001",
				"SIMPLEDBM-WD0001: Table {0} already loaded");
		MessageCatalog
				.addMessage("ID0002",
						"SIMPLEDBM-ID0002: Loading definition for table {0} at startup");
		MessageCatalog.addMessage("ED0003",
				"SIMPLEDBM-ED0003: Database is already started");
		MessageCatalog.addMessage("ID0004",
				"SIMPLEDBM-ID0004: SimpleDBM Database startup complete");
		MessageCatalog
				.addMessage(
						"ED0005",
						"SIMPLEDBM-ED0005: The table definition for {0} lacks a primary index definition");
		MessageCatalog.addMessage("ED0006", 
				"SIMPLEDBM-ED0006: A container already exists with specified container Id {0}");
		MessageCatalog.addMessage("ED0007", "SIMPLEDBM-ED0007: A container already exists with the specified name {0}");
		MessageCatalog.addMessage("ED0008", "SIMPLEDBM-ED0008: Table {0} already exists");
		MessageCatalog.addMessage("ED0009", "SIMPLEDBM-ED0009: Unexpected error occurred while reading a log record");
		
	}
	final static MessageCatalog mcat = new MessageCatalog();

	/**
	 * The table cache holds definitions of all tables and associated indexes.
	 * The table cache is updated at system startup, and also every time a new
	 * table is created.
	 */
	ArrayList<TableDefinition> tables = new ArrayList<TableDefinition>();

	/**
	 * Register a table definition to the in-memory dictionary cache. Caller
	 * must protect {@link #tables}.
	 * 
	 * @param tableDefinition
	 */
	private void registerTableDefinition(TableDefinition tableDefinition) {
		/*
		 * Let us check if another thread has already loaded registered this
		 * definition.
		 */
		for (TableDefinition td : tables) {
			if (td.getContainerId() == tableDefinition.getContainerId()) {
				log.warn(DatabaseImpl.class.getName(),
						"registerTableDefinition", mcat.getMessage("WD0001",
								tableDefinition));
				return;
			}
		}
		/* Register the table's type descriptor with the Row Factory */
		getRowFactory().registerRowType(tableDefinition.getContainerId(),
				tableDefinition.getRowType());
		/* For each of the indexes, register the row type descriptor */
		for (IndexDefinition idx : tableDefinition.getIndexes()) {
			getRowFactory().registerRowType(idx.getContainerId(),
					idx.getRowType());
		}
		tables.add(tableDefinition);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.simpledbm.database.Database#newTableDefinition(java.lang.String,
	 * int, org.simpledbm.typesystem.api.TypeDescriptor[])
	 */
	public TableDefinition newTableDefinition(String name, int containerId,
			TypeDescriptor[] rowType) {
		return new TableDefinitionImpl(this, containerId, name, rowType);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.simpledbm.database.Database#getTableDefinition(int)
	 */
	public TableDefinition getTableDefinition(int containerId) {
		synchronized (tables) {
			for (TableDefinition tableDefinition : tables) {
				if (tableDefinition.getContainerId() == containerId) {
					return tableDefinition;
				}
			}
		}
		return null;
	}

	/**
	 * Load definitions of tables at system startup.
	 */
	private void loadTableDefinitionsAtStartup() {
		/*
		 * A table definition is stored in a special container that is named as
		 * <containerId>.def, where containerId is the table's container ID. At
		 * startup, we look at all the open containers and identify the table
		 * definitions by looking at the name of the container.
		 */
		StorageContainerInfo[] containers = getServer().getStorageManager()
				.getActiveContainers();
		StorageContainerFactory factory = getServer().getStorageFactory();
		for (StorageContainerInfo sc : containers) {
			int containerId = sc.getContainerId();
			String tableName = makeTableDefName(containerId);

			/*
			 * If the container is a table definition container, retrieve it
			 * from the storage system.
			 */
			if (factory.exists(tableName)
					&& getTableDefinition(containerId) == null) {
				log.info(getClass().getName(), "loadTableDefinitonsAtStartup",
						mcat.getMessage("ID0002", containerId));
				retrieveTableDefinition(containerId);
			}
		}
	}

	/**
	 * Constructs the Database object. The actual initialization of the server
	 * objects is deferred until the database is started.
	 * 
	 * @param properties
	 *            Properties for the database. For details see RSS
	 *            Documentation.
	 */
	public DatabaseImpl(Properties properties) {
		validateProperties(properties);
		this.properties = properties;
	}

	private void validateProperties(Properties properties2) {
		// TODO Auto-generated method stub
	}

	/**
	 * Creates the Database server on persistent storage.
	 * 
	 * @param properties
	 */
	public static void create(Properties properties) {
		Server.create(properties);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.simpledbm.database.Database#start()
	 */
	public void start() {
		/*
		 * We cannot start the server more than once
		 */
		if (serverStarted) {
			log.error(getClass().getName(), "start", mcat.getMessage("ED0003"));
			throw new DatabaseException(mcat.getMessage("ED0003"));
		}

		/*
		 * We must always create a new server object. Construction sequence is
		 * very important. Before the RSS instance is started, the DatabaseImpl
		 * module must be registered as a transactional module.
		 */
		server = new Server(properties);
		log = Logger.getLogger(DatabaseImpl.class.getPackage().getName());
		/*
		 * Register any objects we need to manage the database. This must be
		 * done prior to starting the database.
		 */
		server.getObjectRegistry().registerSingleton(MODULE_ID, this);
		server.getObjectRegistry().registerType(TYPE_CREATE_TABLE_DEFINITION,
				CreateTableDefinition.class.getName());
		server.registerSingleton(ROW_FACTORY_TYPE_ID, rowFactory);
		/*
		 * Register this module as a transactional module. This will allow the
		 * module to participate in transactions.
		 */
		server.getModuleRegistry().registerModule(MODULE_ID, this);

		/*
		 * Now we are ready to start.
		 */
		server.start();
		/*
		 * Immediately after server startup, we need to load all the table
		 * definitions into our table cache.
		 */
		loadTableDefinitionsAtStartup();

		/*
		 * Finally, let's set the started flag to true.
		 */
		log.info(getClass().getName(), "start", mcat.getMessage("ID0004"));
		serverStarted = true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.simpledbm.database.Database#shutdown()
	 */
	public void shutdown() {
		if (serverStarted) {
			server.shutdown();
			serverStarted = false;
			server = null;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.simpledbm.database.Database#getServer()
	 */
	public Server getServer() {
		return server;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.simpledbm.database.Database#getFieldFactory()
	 */
	public FieldFactory getFieldFactory() {
		return fieldFactory;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.simpledbm.database.Database#getRowFactory()
	 */
	public RowFactory getRowFactory() {
		return rowFactory;
	}

	private void validateTableDefinition(TableDefinition tableDefinition) {
		/*
		 * Check that the table has primary key index
		 */
		if (tableDefinition.getIndexes().size() == 0
				|| !tableDefinition.getIndexes().get(0).isUnique()) {
			log.error(getClass().getName(), "validateTableDefinition", mcat
					.getMessage("ED0005", tableDefinition.getName()));
			throw new DatabaseException(mcat.getMessage("ED0005",
					tableDefinition.getName()));
		}
		/*
		 * Other validations: TODO
		 */
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.simpledbm.database.Database#createTable(org.simpledbm.database.
	 * TableDefinition)
	 */
	public void createTable(TableDefinition tableDefinition) {

		validateTableDefinition(tableDefinition);

		Transaction trx = server.begin(IsolationMode.READ_COMMITTED);
		boolean success = false;
		try {
			/*
			 * First let's lock tuple container to prevent concurrent access
			 */
			getServer().getTupleManager().lockTupleContainer(trx,
					tableDefinition.getContainerId(), LockMode.EXCLUSIVE);
			/*
			 * Lets also lock all the index containers
			 */
			for (IndexDefinition idx : tableDefinition.getIndexes()) {
				server.getIndexManager().lockIndexContainer(trx,
						idx.getContainerId(), LockMode.EXCLUSIVE);
			}

			/*
			 * Okay - but lets also check that none of the containers are active
			 */
			boolean containersActive = false;
			// TODO: Scan through the list of active containers and check that
			// the new containerIds or names
			// do not clash with existing names and ids.
			HashSet<String> namesSet = new HashSet<String>();
			HashSet<Integer> containerIdSet = new HashSet<Integer>();

			namesSet.add(tableDefinition.getName());
			containerIdSet.add(tableDefinition.getContainerId());
			for (IndexDefinition idx : tableDefinition.getIndexes()) {
				namesSet.add(idx.getName());
				containerIdSet.add(idx.getContainerId());
			}
			StorageContainerInfo[] containers = getServer().getStorageManager()
					.getActiveContainers();
			for (StorageContainerInfo sc : containers) {
				if (containerIdSet.contains(sc.getContainerId())) {
					log.error(getClass().getName(), "createTable", mcat.getMessage("ED0006", sc.getContainerId()));
					throw new DatabaseException(mcat.getMessage("ED0006", sc.getContainerId()));
				}
				if (namesSet.contains(sc.getName())) {
					log.error(getClass().getName(), "createTable", mcat.getMessage("ED0007", sc.getName()));
					throw new DatabaseException(mcat.getMessage("ED0007", sc.getName()));
				}
			}

			/*
			 * Now also check whether a table definition already exists. This
			 * should never happen, actually (TODO)
			 */
			synchronized (tables) {
				if (getTableDefinition(tableDefinition.getContainerId()) == null) {
					registerTableDefinition(tableDefinition);
				} else {
					log.error(getClass().getName(), "createTable", mcat.getMessage("ED0008", tableDefinition.getName()));
					throw new DatabaseException(mcat.getMessage("ED0008", tableDefinition.getName()));
				}
			}
			/*
			 * Following will obtain a lock on the containerID.
			 */
			server.createTupleContainer(trx, tableDefinition.getName(),
					tableDefinition.getContainerId(), 8);
			for (IndexDefinition idx : tableDefinition.getIndexes()) {
				server.createIndex(trx, idx.getName(), idx.getContainerId(), 8,
						ROW_FACTORY_TYPE_ID, idx.isUnique());
			}
			CreateTableDefinition ctd = (CreateTableDefinition) server
					.getLoggableFactory().getInstance(MODULE_ID,
							TYPE_CREATE_TABLE_DEFINITION);
			ctd.database = this;
			ctd.table = tableDefinition;
			trx.schedulePostCommitAction(ctd);
			success = true;
		} finally {
			if (success) {
				trx.commit();
			} else {
				trx.abort();
			}
		}
	}

	@Override
	public void redo(Page page, Redoable loggable) {
		/* No Op */
	}

	@Override
	public void redo(Loggable loggable) {
		if (loggable instanceof CreateTableDefinition) {
			CreateTableDefinition ctd = (CreateTableDefinition) loggable;
			storeTableDefinition(ctd.table);
		}
	}

	/**
	 * Constructs a name for the container that will store the table and
	 * associated index definitions. The container name will be based upon
	 * the table's container ID. This naming convention allows the definition
	 * to be loaded if the container ID is known. Since SimpleDBM maintains a
	 * list of open containers, it is possible to use this information to build
	 * the data dictionary at system start up.
	 * 
	 * @param containerId The container ID for the table
	 * @return The name of the container that will hold the table definition
	 */
	private String makeTableDefName(int containerId) {
		return "_internal/" + containerId + ".def";
	}

	/**
	 * Makes the table definition persistent by storing it as in a container.
	 * 
	 * @see #makeTableDefName(int)
	 * @param tableDefinition
	 *            The Table Definition to be persisted
	 */
	private void storeTableDefinition(TableDefinition tableDefinition) {
		String tableName = makeTableDefName(tableDefinition.getContainerId());
		StorageContainerFactory storageFactory = server.getStorageFactory();
		StorageContainer sc = storageFactory.createIfNotExisting(tableName);
		try {
			byte buffer[] = new byte[tableDefinition.getStoredLength() + TypeSize.INTEGER];
			ByteBuffer bb = ByteBuffer.wrap(buffer);
			bb.putInt(buffer.length);
			tableDefinition.store(bb);
			sc.write(0, buffer, 0, buffer.length);
		} finally {
			sc.close();
		}
	}

	/**
	 * Retrieve a table definition from storage and register the table
	 * definition.
	 * 
	 * @see #makeTableDefName(int)
	 * @param containerId
	 *            The container ID of the table's tuple container
	 * @return The Table Definition of the specified table.
	 */
	TableDefinition retrieveTableDefinition(int containerId) {
		String tableName = makeTableDefName(containerId);
		StorageContainerFactory storageFactory = server.getStorageFactory();
		StorageContainer sc = storageFactory.open(tableName);
		TableDefinitionImpl table = null;
		try {
			byte buffer[] = new byte[TypeSize.INTEGER];
			sc.read(0, buffer, 0, buffer.length);
			ByteBuffer bb = ByteBuffer.wrap(buffer);
			int n = bb.getInt();
			buffer = new byte[n];
			sc.read(TypeSize.INTEGER, buffer, 0, buffer.length);
			bb = ByteBuffer.wrap(buffer);
			table = new TableDefinitionImpl(this);
			table.retrieve(bb);
		} finally {
			sc.close();
		}
		synchronized (tables) {
			registerTableDefinition(table);
		}
		return table;
	}

	/**
	 * Responsible for adding the table definition to the data dictionary.
	 * The persistence of the table definition is handled as a post commit action.
	 * Before the table definition is logged, the table and associated indexes are
	 * created. 
	 * 
	 * @author dibyendumajumdar
	 * @since 29 Dec 2007
	 */
	public static final class CreateTableDefinition extends BaseLoggable
			implements PostCommitAction, ObjectRegistryAware {

		int actionId;
		TableDefinition table;
		Database database;
		ObjectRegistry objectRegistry;

		public CreateTableDefinition() {
		}

		public CreateTableDefinition(TableDefinitionImpl table) {
			this.table = table;
			this.database = table.getDatabase();
			this.objectRegistry = table.getDatabase().getServer()
					.getObjectRegistry();
		}

		@Override
		public void init() {
		}

		public int getActionId() {
			return actionId;
		}

		public void setActionId(int actionId) {
			this.actionId = actionId;
		}

		@Override
		public StringBuilder appendTo(StringBuilder sb) {
			sb.append("CreateTableDefinition(");
			super.appendTo(sb);
			sb.append(", actionId=");
			sb.append(actionId);
			sb.append(", table=");
			return sb;
		}

		/* (non-Javadoc)
		 * @see org.simpledbm.rss.api.tx.BaseLoggable#getStoredLength()
		 */
		@Override
		public int getStoredLength() {
			int n = super.getStoredLength();
			n += TypeSize.INTEGER;
			n += table.getStoredLength();
			return n;
		}

		/* (non-Javadoc)
		 * @see org.simpledbm.rss.api.tx.BaseLoggable#retrieve(java.nio.ByteBuffer)
		 */
		@Override
		public void retrieve(ByteBuffer bb) {
			if (database == null) {
				log.error(getClass().getName(), "retrieve", mcat.getMessage("ED0009"));
				throw new DatabaseException(mcat.getMessage("ED0009"));
			}
			super.retrieve(bb);
			actionId = bb.getInt();
			table = new TableDefinitionImpl(database);
			table.retrieve(bb);
		}

		/* (non-Javadoc)
		 * @see org.simpledbm.rss.api.tx.BaseLoggable#store(java.nio.ByteBuffer)
		 */
		@Override
		public void store(ByteBuffer bb) {
			super.store(bb);
			bb.putInt(actionId);
			table.store(bb);
		}

		@Override
		public String toString() {
			return appendTo(new StringBuilder()).toString();
		}

		/* (non-Javadoc)
		 * @see org.simpledbm.rss.api.registry.ObjectRegistryAware#setObjectFactory(org.simpledbm.rss.api.registry.ObjectRegistry)
		 */
		public void setObjectFactory(ObjectRegistry objectFactory) {
			this.objectRegistry = objectFactory;
			database = (Database) objectRegistry
					.getInstance(DatabaseImpl.MODULE_ID);
			if (database == null) {
				log.error(getClass().getName(), "retrieve", mcat.getMessage("ED0009"));
				throw new DatabaseException(mcat.getMessage("ED0009"));
			}		
		}
	}

	/* (non-Javadoc)
	 * @see org.simpledbm.database.api.Database#getTable(org.simpledbm.database.api.TableDefinition)
	 */
	public Table getTable(TableDefinition tableDefinition) {
		return new TableImpl(tableDefinition);
	}
}
