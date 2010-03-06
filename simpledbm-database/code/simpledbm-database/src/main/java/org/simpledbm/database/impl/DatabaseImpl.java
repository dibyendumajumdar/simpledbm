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
package org.simpledbm.database.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;

import org.simpledbm.common.api.exception.ExceptionHandler;
import org.simpledbm.common.api.locking.LockMode;
import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.api.registry.ObjectFactory;
import org.simpledbm.common.api.tx.IsolationMode;
import org.simpledbm.common.impl.platform.PlatformImpl;
import org.simpledbm.common.util.TypeSize;
import org.simpledbm.common.util.logging.Logger;
import org.simpledbm.common.util.mcat.Message;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.common.util.mcat.MessageType;
import org.simpledbm.database.api.Database;
import org.simpledbm.database.api.Table;
import org.simpledbm.exception.DatabaseException;
import org.simpledbm.rss.api.bm.BufferAccessBlock;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.st.StorageContainerInfo;
import org.simpledbm.rss.api.tx.BaseLoggable;
import org.simpledbm.rss.api.tx.BaseTransactionalModule;
import org.simpledbm.rss.api.tx.Compensation;
import org.simpledbm.rss.api.tx.Loggable;
import org.simpledbm.rss.api.tx.PostCommitAction;
import org.simpledbm.rss.api.tx.Redoable;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.rss.api.tx.Undoable;
import org.simpledbm.rss.api.wal.Lsn;
import org.simpledbm.rss.main.Server;
import org.simpledbm.typesystem.api.DictionaryCache;
import org.simpledbm.typesystem.api.IndexDefinition;
import org.simpledbm.typesystem.api.RowFactory;
import org.simpledbm.typesystem.api.TableDefinition;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.api.TypeFactory;
import org.simpledbm.typesystem.api.TypeSystemFactory;
import org.simpledbm.typesystem.impl.TypeSystemFactoryImpl;

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
	 * present indexes cannot be added at a later stage.
	 */

	/* ID for this module */
	final static int MODULE_ID = 100;
	final static int MODULE_BASE = 101;
	
	/** Object registry id for row factory */
	final static int ROW_FACTORY_TYPE_ID = MODULE_BASE + 1;
	/** Type ID for Loggable object */
    final static int TYPE_CREATE_TABLE_DEFINITION = MODULE_BASE + 2;
    final static int TYPE_UNDO_CREATE_TABLE_DEFINITION = MODULE_BASE + 3;
    final static int TYPE_DROP_TABLE_DEFINITION = MODULE_BASE + 4;
    
	/** RSS Server object */
	Server server;
	/** Database startup/create properties */
	Properties properties;
	/** Flag to indicate whether the database has been started */
	private boolean serverStarted = false;
	/** The TypeSystem Factory we will use for constructing types */
	final TypeSystemFactory typeSystemFactory;
	/** TypeFactory for creating various data types */
	final TypeFactory fieldFactory;
	/** Dictonary cache for maintaining all table definitions in memory */
	final DictionaryCache dictionaryCache = new DictionaryCacheImpl(this); 
	/** The RowFactory we will use for constructing rows */
	final RowFactory rowFactory;

	final Platform platform;
	final PlatformObjects po;
	final Logger log;
	final ExceptionHandler exceptionHandler;
	
	/**
	 * The table cache holds definitions of all tables and associated indexes.
	 * The table cache is updated at system startup, and also every time a new
	 * table is created.
	 */
	ArrayList<TableDefinition> tables = new ArrayList<TableDefinition>();

	static final Message m_WD0001 = new Message('D', 'D', MessageType.WARN, 1,
			"Table {0} already loaded");
	static final Message m_ID0002 = new Message('D', 'D', MessageType.INFO, 2,
			"Loading definition for table {0} at startup");
	static final Message m_ED0003 = new Message('D', 'D', MessageType.ERROR, 3,
			"Database is already started");
	static final Message m_ID0004 = new Message('D', 'D', MessageType.INFO, 4,
			"SimpleDBM Database startup complete");
	static final Message m_ED0005 = new Message('D', 'D', MessageType.ERROR, 5,
			"The table definition for {0} lacks a primary index definition");
	static final Message m_ED0006 = new Message('D', 'D', MessageType.ERROR, 6,
			"A container already exists with specified container Id {0}");
	static final Message m_ED0007 = new Message('D', 'D', MessageType.ERROR, 7,
			"A container already exists with the specified name {0}");
	static final Message m_ED0008 = new Message('D', 'D', MessageType.ERROR, 8,
			"Table {0} already exists");
	static final Message m_ED0009 = new Message('D', 'D', MessageType.ERROR, 9,
			"Unexpected error occurred while reading a log record");
	static final Message m_ED0013 = new Message('D', 'D', MessageType.ERROR,
			13,
			"Unexpected error occurred while reading the data dictionary for container {0}");
	static final Message m_ED0014 = new Message('D', 'D', MessageType.ERROR,
			14, "Cannot update row as primary key is different");
	static final Message m_ED0015 = new Message('D', 'D', MessageType.ERROR,
			15, "Row failed validation: {0}");
	
	
	public DictionaryCache getDictionaryCache() {
		return dictionaryCache;
	}
	
	public PlatformObjects getPlatformObjects() {
		return po;
	}
	
	/**
	 * Register a table definition to the in-memory dictionary cache. Caller
	 * must protect {@link #tables}.
	 * 
	 * @param tableDefinition
	 */
	private void registerTableDefinition(TableDefinition tableDefinition) {
		/*
		 * Let us check if another thread has already loaded registered this
		 * definition. It is also possible that the table may have been loaded
		 * during restart recovery.
		 */
		for (TableDefinition td : tables) {
			if (td.getContainerId() == tableDefinition.getContainerId()) {
				log.warn(DatabaseImpl.class.getName(),
						"registerTableDefinition", new MessageInstance(m_WD0001,
								tableDefinition).toString());
				return;
			}
		}
		/* Register the table's type descriptor with the Row Factory */
		getDictionaryCache().registerRowType(tableDefinition.getContainerId(),
				tableDefinition.getRowType());
		/* For each of the indexes, register the row type descriptor */
		for (IndexDefinition idx : tableDefinition.getIndexes()) {
			getDictionaryCache().registerRowType(idx.getContainerId(),
					idx.getRowType());
		}
		tables.add(tableDefinition);
	}

    /**
     * Removes a table definition to the in-memory dictionary cache. Caller
     * must protect {@link #tables}.
     * 
     * @param tableDefinition
     */
    private void unregisterTableDefinition(int containerId) {
        Iterator<TableDefinition> iter = tables.iterator();
        TableDefinition tableDefinition = null;
        while (iter.hasNext()) {
            TableDefinition td = iter.next();
            if (td.getContainerId() == containerId) {
                tableDefinition = td;
                iter.remove();
                break;
            }
        }
        if (tableDefinition == null) {
        	exceptionHandler.errorThrow(getClass().getName(), "unregisterTableDefinition", 
        			new DatabaseException(new MessageInstance(m_ED0013, containerId)));
        }
        /* Remove the table's type descriptor from the Row Factory */
        getDictionaryCache().unregisterRowType(tableDefinition.getContainerId());
        /* Remove the indexes */
        for (IndexDefinition idx : tableDefinition.getIndexes()) {
            getDictionaryCache().unregisterRowType(idx.getContainerId());
        }
    }
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.simpledbm.database.Database#newTableDefinition(java.lang.String,
	 * int, org.simpledbm.typesystem.api.TypeDescriptor[])
	 */
	public TableDefinition newTableDefinition(String name, int containerId,
			TypeDescriptor[] rowType) {
		return typeSystemFactory.getTableDefinition(po, fieldFactory, rowFactory, containerId, name, rowType);
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
						new MessageInstance(m_ID0002, containerId).toString());
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
		platform = new PlatformImpl(properties);
		po = platform.getPlatformObjects(Database.LOGGER_NAME);
		log = po.getLogger();
		exceptionHandler = po.getExceptionHandler();
		typeSystemFactory = new TypeSystemFactoryImpl(properties, po);
		fieldFactory = typeSystemFactory.getDefaultTypeFactory();
		rowFactory = typeSystemFactory.getDefaultRowFactory(fieldFactory, dictionaryCache);
	}

	/**
	 * Constructs the Database object. The actual initialization of the server
	 * objects is deferred until the database is started.
	 * 
	 * @param properties
	 *            Properties for the database. For details see RSS
	 *            Documentation.
	 */
	public DatabaseImpl(Platform platform, Properties properties) {
		validateProperties(properties);
		this.properties = properties;
		this.platform = platform; 
		po = platform.getPlatformObjects(Database.LOGGER_NAME);
		log = po.getLogger();
		exceptionHandler = po.getExceptionHandler();
		typeSystemFactory = new TypeSystemFactoryImpl(properties, po);
		fieldFactory = typeSystemFactory.getDefaultTypeFactory();
		rowFactory = typeSystemFactory.getDefaultRowFactory(fieldFactory, dictionaryCache);
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
			exceptionHandler.errorThrow(getClass().getName(), "start", 
					new DatabaseException(new MessageInstance(m_ED0003)));
		}

		/*
		 * We must always create a new server object. Construction sequence is
		 * very important. Before the RSS instance is started, the DatabaseImpl
		 * module must be registered as a transactional module.
		 */
		server = new Server(platform, properties);
		/*
		 * Register any objects we need to manage the database. This must be
		 * done prior to starting the database.
		 */
		server.getObjectRegistry().registerSingleton(MODULE_ID, this);
		server.getObjectRegistry().registerObjectFactory(TYPE_CREATE_TABLE_DEFINITION,
				new CreateTableDefinition.CreateTableDefinitionFactory(this));
        server.getObjectRegistry().registerObjectFactory(
                TYPE_UNDO_CREATE_TABLE_DEFINITION,
                new UndoCreateTableDefinition.UndoCreateTableDefinitionFactory(
                        this));
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
		log.info(getClass().getName(), "start", new MessageInstance(m_ID0004).toString());
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
	
	public TypeSystemFactory getTypeSystemFactory() {
		return typeSystemFactory;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.simpledbm.database.Database#getFieldFactory()
	 */
	public TypeFactory getTypeFactory() {
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

	/* (non-Javadoc)
	 * @see org.simpledbm.database.api.Database#startTransaction(org.simpledbm.rss.api.tx.IsolationMode)
	 */
	public Transaction startTransaction(IsolationMode isolationMode) {
		return getServer().begin(isolationMode);
	}

	private void validateTableDefinition(TableDefinition tableDefinition) {
		/*
		 * Check that the table has primary key index
		 */
		if (tableDefinition.getIndexes().size() == 0
				|| !tableDefinition.getIndexes().get(0).isUnique()) {
			exceptionHandler.errorThrow(getClass().getName(), "validateTableDefinition", 
				new DatabaseException(new MessageInstance(m_ED0005,
					tableDefinition.getName())));
		}
		/*
		 * Other validations: TODO
		 */
	}

	public Table getTable(Transaction trx, int containerId) {
		/*
		 * First let's lock tuple container to prevent table from being
		 * deleted while we have access to it.
		 */
		getServer().getTupleManager().lockTupleContainer(trx,
				containerId, LockMode.SHARED);
		return getTable(getTableDefinition(containerId));	
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
			 * 
			 * Scan through the list of active containers and check that the new
			 * containerIds or names do not clash with existing names and ids.
			 */
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
					exceptionHandler.errorThrow(getClass().getName(), "createTable", 
						new DatabaseException(new MessageInstance(m_ED0006, sc.getContainerId())));
				}
				if (namesSet.contains(sc.getName())) {
					exceptionHandler.errorThrow(getClass().getName(), "createTable", 
						new DatabaseException(new MessageInstance(m_ED0007, sc.getName())));
				}
			}

			/*
			 * Add the table definition to the dictionary cache.
			 * Also check whether a table definition already exists. This
			 * should never happen, actually.
			 */
			synchronized (tables) {
				if (getTableDefinition(tableDefinition.getContainerId()) == null) {
					registerTableDefinition(tableDefinition);
				} else {
					exceptionHandler.errorThrow(getClass().getName(), "createTable", 
						new DatabaseException(new MessageInstance(m_ED0008, tableDefinition.getName())));
				}
			}

			CreateTableDefinition ctd = new CreateTableDefinition(MODULE_ID,
					TYPE_CREATE_TABLE_DEFINITION, this);
			ctd.table = tableDefinition;
			/*
			 * Normal redo log records must be logged against a page. As we do not
			 * have a page for the data dictionary entry - we log it against the
			 * virtual table page (0,0).
			 */
			BufferAccessBlock bab = server.getBufferManager().fixExclusive(
					new PageId(0, 0), false, server.getPageFactory().getRawPageType(), 0);
			try {
				redo(bab.getPage(), ctd);
				Lsn lsn = trx.logInsert(bab.getPage(), ctd);
				bab.setDirty(lsn);
			}
			finally {
				bab.unfix();
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
			success = true;
		} finally {
			if (success) {
				trx.commit();
			} else {
				trx.abort();
			}
		}
	}

    public void dropTable(TableDefinition tableDefinition) {
    	
    	/*
    	 * All drop operations are handled as post commit actions 
    	 * as recovery of a deleted container would be very difficult.
    	 * We only delete a container when are sure that the transaction will
    	 * commit.
    	 */
    	
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
             * Drop all indexes
             * FIXME we should drop indexes in reverse order of creation
             */
            for (IndexDefinition idx : tableDefinition.getIndexes()) {
                server.getSpaceManager().dropContainer(trx,
                        idx.getContainerId());
            }
            /*
             * Drop the table
             */
            server.getSpaceManager().dropContainer(trx,
                    tableDefinition.getContainerId());

            /*
             * Drop the definitions from the data dictionary.
             */
            DropTableDefinition dtp = new DropTableDefinition(MODULE_ID,
                    TYPE_DROP_TABLE_DEFINITION, this, tableDefinition);
            trx.schedulePostCommitAction(dtp);

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
        if (loggable instanceof CreateTableDefinition) {
            CreateTableDefinition ctd = (CreateTableDefinition) loggable;
            storeTableDefinition(ctd.table);
        } else if (loggable instanceof UndoCreateTableDefinition) {
            UndoCreateTableDefinition uctd = (UndoCreateTableDefinition) loggable;
            dropTableDefinition(uctd);
        }
    }

    @Override
    public void redo(Loggable loggable) {
        if (loggable instanceof DropTableDefinition) {
            DropTableDefinition uctd = (DropTableDefinition) loggable;
            dropTableDefinition(uctd);
        }
    }

    @Override
    public Compensation generateCompensation(Undoable undoable) {
        if (undoable instanceof CreateTableDefinition) {
            UndoCreateTableDefinition undoCreateTableDefinition = new UndoCreateTableDefinition(
                    MODULE_ID, TYPE_UNDO_CREATE_TABLE_DEFINITION, this);
            CreateTableDefinition ctd = (CreateTableDefinition) undoable;
            undoCreateTableDefinition.defList.add(ctd.table.getContainerId());
            for (IndexDefinition id : ctd.table.getIndexes()) {
                undoCreateTableDefinition.defList.add(id.getContainerId());
            }
            return undoCreateTableDefinition;
        }
        return null;
    }
	
    /**
     * Deletes the table and index definitions.
     */
    private void dropTableDefinition(DeleteTableDefinition deleteTableDefinition) {
        for (int c : deleteTableDefinition.defList) {
            String containerName = makeTableDefName(c);
            if (log.isDebugEnabled()) {
                log.debug(getClass().getName(), "dropTableDefinition",
                        "Dropping definition " + containerName);
            }
            getServer().getStorageFactory().delete(containerName);
        }
        synchronized (tables) {
            /*
             * unregister table definition: note the assumption that the first
             * item in the list is the table's container ID.
             */
            unregisterTableDefinition(deleteTableDefinition.defList.get(0));
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
		StorageContainer sc = storageFactory.create(tableName);
		try {
			int n = tableDefinition.getStoredLength();
			byte buffer[] = new byte[n + TypeSize.INTEGER + TypeSize.BYTE];
			ByteBuffer bb = ByteBuffer.wrap(buffer);
			bb.put((byte) 1);
			bb.putInt(n);
			tableDefinition.store(bb);
			sc.write(0, buffer, 0, buffer.length);
		} finally {
			sc.close();
		}
		for (IndexDefinition idx : tableDefinition.getIndexes()) {
			storeIndexDefinition(idx);
		}
	}
	
	private void storeIndexDefinition(IndexDefinition indexDefinition) {
		/*
		 * For indexes, we store a pointer to the table definition.
		 */
		String indexName = makeTableDefName(indexDefinition.getContainerId());
		StorageContainerFactory storageFactory = server.getStorageFactory();
		StorageContainer sc = storageFactory.create(indexName);
		try {
			byte buffer[] = new byte[TypeSize.INTEGER + TypeSize.BYTE];
			ByteBuffer bb = ByteBuffer.wrap(buffer);
			bb.put((byte) 2);
			bb.putInt(indexDefinition.getTable().getContainerId());
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
		TableDefinition table = null;
		try {
			byte buffer[] = new byte[TypeSize.BYTE + TypeSize.INTEGER];
			sc.read(0, buffer, 0, buffer.length);
			ByteBuffer bb = ByteBuffer.wrap(buffer);
			byte b = bb.get();
			int n = bb.getInt();
			if (b == ((byte)2)) {
				/*
				 * Index definition, so n must be the table definition's container ID. We
				 * could save reading the table definition by checking whether it is already
				 * cached - but for now, this should work.
				 */
				return retrieveTableDefinition(n);
			}
			buffer = new byte[n];
			sc.read(TypeSize.BYTE + TypeSize.INTEGER, buffer, 0, buffer.length);
			bb = ByteBuffer.wrap(buffer);
			table = typeSystemFactory.getTableDefinition(po, fieldFactory, rowFactory, bb);
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
	 * 
	 * @author dibyendumajumdar
	 * @since 29 Dec 2007
	 */
	public static final class CreateTableDefinition extends BaseLoggable
			implements Undoable {

		TableDefinition table;
		final DatabaseImpl database;

		public CreateTableDefinition(DatabaseImpl database, ByteBuffer bb) {
			super(bb);
			this.database = database;
			table = database.typeSystemFactory.getTableDefinition(database.po, database.fieldFactory, database.rowFactory, bb);
		}

		public CreateTableDefinition(int moduleId, int typeCode, DatabaseImpl database) {
			super(moduleId, typeCode);
			this.database = database;
		}

		@Override
		public StringBuilder appendTo(StringBuilder sb) {
			sb.append("CreateTableDefinition(");
			super.appendTo(sb);
			sb.append(", table=");
			table.appendTo(sb);
			sb.append(")");
			return sb;
		}

		/* (non-Javadoc)
		 * @see org.simpledbm.rss.api.tx.BaseLoggable#getStoredLength()
		 */
		@Override
		public int getStoredLength() {
			int n = super.getStoredLength();
			n += table.getStoredLength();
			return n;
		}

		/* (non-Javadoc)
		 * @see org.simpledbm.rss.api.tx.BaseLoggable#store(java.nio.ByteBuffer)
		 */
		@Override
		public void store(ByteBuffer bb) {
			super.store(bb);
			table.store(bb);
		}

		@Override
		public String toString() {
			return appendTo(new StringBuilder()).toString();
		}

		static final class CreateTableDefinitionFactory implements ObjectFactory {
			private final DatabaseImpl database;
			CreateTableDefinitionFactory(DatabaseImpl database) {
				this.database = database;
			}
			public Class<?> getType() {
				return CreateTableDefinition.class;
			}
			public Object newInstance(ByteBuffer bb) {
				return new CreateTableDefinition(database, bb);
			}
		}
	}

    /**
     * Abstract log record for dropping table and its indexes.
     */
    static abstract class DeleteTableDefinition extends BaseLoggable {

        final DatabaseImpl database;
        ArrayList<Integer> defList = new ArrayList<Integer>();

        protected DeleteTableDefinition(DatabaseImpl database, ByteBuffer bb) {
            super(bb);
            this.database = database;
            int n = bb.getShort();
            defList.clear();
            for (int i = 0; i < n; i++) {
                defList.add((int) bb.getShort());
            }
        }

        public DeleteTableDefinition(int moduleId, int typecode,
                DatabaseImpl database) {
            super(moduleId, typecode);
            this.database = database;
        }

        @Override
        public StringBuilder appendTo(StringBuilder sb) {
            return super.appendTo(sb).append(", containerList=")
                    .append(defList);
        }

        @Override
        public int getStoredLength() {
            int n = super.getStoredLength();
            n += ((defList.size() + 1) * TypeSize.SHORT);
            return n;
        }

        @Override
        public void store(ByteBuffer bb) {
            super.store(bb);
            bb.putShort((short) defList.size());
            for (int i : defList) {
                bb.putShort((short) i);
            }
        }

        @Override
        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }
    }

    /**
     * Logs undo of a table creation.
     */
    public static final class UndoCreateTableDefinition extends
            DeleteTableDefinition implements Compensation {

        public UndoCreateTableDefinition(DatabaseImpl database, ByteBuffer bb) {
            super(database, bb);
        }

        public UndoCreateTableDefinition(int moduleId, int typecode,
                DatabaseImpl database) {
            super(moduleId, typecode, database);
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("UndoCreateTableDefinition(");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }

        @Override
        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }

        static final class UndoCreateTableDefinitionFactory implements
                ObjectFactory {
            private final DatabaseImpl database;

            UndoCreateTableDefinitionFactory(DatabaseImpl database) {
                this.database = database;
            }

            public Class<?> getType() {
                return UndoCreateTableDefinition.class;
            }

            public Object newInstance(ByteBuffer buf) {
                return new UndoCreateTableDefinition(database, buf);
            }
        }
    }

    /**
     * Logs the dropping of a table and its associated indexes. Handled as post
     * commit action.
     */
    public static final class DropTableDefinition extends DeleteTableDefinition
            implements PostCommitAction {

        int actionId;

        public DropTableDefinition(DatabaseImpl database, ByteBuffer bb) {
            super(database, bb);
        }

        public DropTableDefinition(int moduleId, int typecode,
                DatabaseImpl database, TableDefinition tableDefinition) {
            super(moduleId, typecode, database);
            /*
             * The order in which the definitions are added is important as the
             * first item in the list is expected to be the table's container
             * ID. If this changes, then the method dropTableDefinition() will
             * need to be changed.
             */
            defList.add(tableDefinition.getContainerId());
            for (IndexDefinition idx : tableDefinition.getIndexes()) {
                defList.add(idx.getContainerId());
            }
        }

        public int getActionId() {
            return actionId;
        }

        public void setActionId(int actionId) {
            this.actionId = actionId;
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("DropTableDefinition(");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }

        @Override
        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }

        static final class DropTableDefinitionFactory implements ObjectFactory {
            private final DatabaseImpl database;

            DropTableDefinitionFactory(DatabaseImpl database) {
                this.database = database;
            }

            public Class<?> getType() {
                return DropTableDefinition.class;
            }

            public Object newInstance(ByteBuffer buf) {
                return new DropTableDefinition(database, buf);
            }
        }
    }
    
	/**
	 * Obtains an instance of the Table associated with the supplied
	 * TableDefinition.
	 * 
	 * @param tableDefinition
	 * @return Table object representing the table
	 */	
	Table getTable(TableDefinition tableDefinition) {
		return new TableImpl(po, this, tableDefinition);
	}
}
