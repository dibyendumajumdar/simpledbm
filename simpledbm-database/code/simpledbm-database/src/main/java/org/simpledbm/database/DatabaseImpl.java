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

import org.simpledbm.rss.api.exception.RSSException;
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

public class DatabaseImpl extends BaseTransactionalModule implements Database {

    final static int MODULE_ID = 100;
    final static int MODULE_BASE = 101;
    /** Object registry id for row factory */
    final static int ROW_FACTORY_TYPE_ID = MODULE_BASE + 1;
    final static int TYPE_CREATE_TABLE_DEFINITION = MODULE_BASE + 2;
    Server server;
    Properties properties;
    private boolean serverStarted = false;
    final FieldFactory fieldFactory = new DefaultFieldFactory();
    final RowFactory rowFactory = new DatabaseRowFactory(this, fieldFactory);
    
    static Logger log = Logger.getLogger(DatabaseImpl.class.getPackage().getName());

    static {
    	MessageCatalog.addMessage("WD0001",
                "SIMPLEDBM-WD0001: Table {0} already loaded");
    	MessageCatalog.addMessage("ID0002",
    		"SIMPLEDBM-ID0002: Loading definition for table {0} at startup");    
    }
    final MessageCatalog mcat = new MessageCatalog();
    
    
    /**
     * The table cache holds definitions of all tables and associated
     * indexes.
     */
    ArrayList<TableDefinition> tables = new ArrayList<TableDefinition>();

    /**
     * Register a table definition to the in-memory dictionary cache.
     * Caller must protect {@link #tables}.
     * @param tableDefinition
     */
    private void registerTableDefinition(TableDefinition tableDefinition) {
        /*
         * Let us check if another thread has already loaded registered
         * this definition.
         */
        for (TableDefinition td : tables) {
            if (td.getContainerId() == tableDefinition.getContainerId()) {
            	log.warn(DatabaseImpl.class.getName(), "registerTableDefinition", mcat.getMessage("WD0001", tableDefinition));
                return;
            }
        }
        getRowFactory().registerRowType(tableDefinition.getContainerId(), tableDefinition.getRowType());
        for (IndexDefinition idx : tableDefinition.getIndexes()) {
            getRowFactory().registerRowType(idx.getContainerId(), idx.getRowType());
        }
        tables.add(tableDefinition);
    }

    /* (non-Javadoc)
	 * @see org.simpledbm.database.Database#newTableDefinition(java.lang.String, int, org.simpledbm.typesystem.api.TypeDescriptor[])
	 */
    public TableDefinition newTableDefinition(String name, int containerId,
            TypeDescriptor[] rowType) {
        return new TableDefinition(this, containerId, name, rowType);
    }

    /* (non-Javadoc)
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
		StorageContainerInfo[] containers = getServer().getStorageManager()
				.getActiveContainers();
		StorageContainerFactory factory = getServer().getStorageFactory();
		for (StorageContainerInfo sc : containers) {
			int containerId = sc.getContainerId();
			String tableName = makeTableDefName(containerId);

			if (factory.exists(tableName)
					&& getTableDefinition(containerId) == null) {
				log.info(getClass().getName(), "loadTableDefinitonsAtStartup", mcat.getMessage("ID0002", containerId));
				retrieveTableDefinition(containerId);
			}
		}
	}

    public DatabaseImpl(Properties properties) {
        validateProperties(properties);
        this.properties = properties;
    }

    private void validateProperties(Properties properties2) {
    // TODO Auto-generated method stub
    }

    public static void create(Properties properties) {
        Server.create(properties);
    }

    /* (non-Javadoc)
	 * @see org.simpledbm.database.Database#start()
	 */
    public void start() {
        /*
         * We cannot start the server more than once
         */
        if (serverStarted) {
            throw new RSSException("Server is already started");
        }

        /*
         * We must always create a new server object.
         */
        server = new Server(properties);
        log = Logger.getLogger(DatabaseImpl.class.getPackage().getName());
        server.getObjectRegistry().registerSingleton(MODULE_ID, this);
        server.getObjectRegistry().registerType(TYPE_CREATE_TABLE_DEFINITION, CreateTableDefinition.class.getName());
        server.getModuleRegistry().registerModule(MODULE_ID, this);
        registerRowFactory();
        server.start();
        loadTableDefinitionsAtStartup();

        serverStarted = true;
    }

    private void registerRowFactory() {
        server.registerSingleton(ROW_FACTORY_TYPE_ID, rowFactory);
    }

    /* (non-Javadoc)
	 * @see org.simpledbm.database.Database#shutdown()
	 */
    public void shutdown() {
        if (serverStarted) {
            server.shutdown();
            serverStarted = false;
            server = null;
        }
    }

    /* (non-Javadoc)
	 * @see org.simpledbm.database.Database#getServer()
	 */
    public Server getServer() {
        return server;
    }

    /* (non-Javadoc)
	 * @see org.simpledbm.database.Database#getFieldFactory()
	 */
    public FieldFactory getFieldFactory() {
        return fieldFactory;
    }

    /* (non-Javadoc)
	 * @see org.simpledbm.database.Database#getRowFactory()
	 */
    public RowFactory getRowFactory() {
        return rowFactory;
    }

    /* (non-Javadoc)
	 * @see org.simpledbm.database.Database#createTable(org.simpledbm.database.TableDefinition)
	 */
    public void createTable(TableDefinition tableDefinition) {
        Transaction trx = server.begin(IsolationMode.READ_COMMITTED);
        boolean success = false;
        try {
            // Lock tuple container to prevent concurrent access
            getServer().getTupleManager().lockTupleContainer(trx, tableDefinition.getContainerId(), LockMode.EXCLUSIVE);

            synchronized (tables) {
                if (getTableDefinition(tableDefinition.getContainerId()) == null) {
                    registerTableDefinition(tableDefinition);
                } else {
                    throw new RSSException("Table already exists");
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
            CreateTableDefinition ctd = (CreateTableDefinition) server.getLoggableFactory().getInstance(MODULE_ID, TYPE_CREATE_TABLE_DEFINITION);
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

    private String makeTableDefName(int containerId) {
    	return "_internal/" + containerId + ".def";
    }
    
    /**
     * Makes the table definition persistent by storing it as a 
     * container.
     * @param table The Table Definition to be persisted
     */
    private void storeTableDefinition(TableDefinition table) {
        String tableName = makeTableDefName(table.getContainerId());
        StorageContainerFactory storageFactory = server.getStorageFactory();
        StorageContainer sc = storageFactory.createIfNotExisting(tableName);
        try {
            byte buffer[] = new byte[table.getStoredLength() + TypeSize.INTEGER];
            ByteBuffer bb = ByteBuffer.wrap(buffer);
            bb.putInt(buffer.length);
            table.store(bb);
            sc.write(0, buffer, 0, buffer.length);
        } finally {
            sc.close();
        }
    }

    /**
     * Retrieve a table definition from storage and register the table
     * definition.
     * 
     * @param containerId The container ID of the table's tuple container
     * @return The Table Definition of the specified table.
     */
    TableDefinition retrieveTableDefinition(int containerId) {

        String tableName = makeTableDefName(containerId);
        StorageContainerFactory storageFactory = server.getStorageFactory();
        StorageContainer sc = storageFactory.open(tableName);
        TableDefinition table = null;
        try {
            byte buffer[] = new byte[TypeSize.INTEGER];
            sc.read(0, buffer, 0, buffer.length);
            ByteBuffer bb = ByteBuffer.wrap(buffer);
            int n = bb.getInt();
            buffer = new byte[n];
            sc.read(TypeSize.INTEGER, buffer, 0, buffer.length);
            bb = ByteBuffer.wrap(buffer);
            table = new TableDefinition(this);
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
     * 
     * @author dibyendumajumdar
     * @since 29 Dec 2007
     */
    public static final class CreateTableDefinition extends BaseLoggable implements PostCommitAction, ObjectRegistryAware {

        int actionId;
        TableDefinition table;
        Database database;
        ObjectRegistry objectRegistry;

        public CreateTableDefinition() {
        }

        public CreateTableDefinition(TableDefinition table) {
            this.table = table;
            this.database = table.getDatabase();
            this.objectRegistry = table.getDatabase().getServer().getObjectRegistry();
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

        @Override
        public int getStoredLength() {
            int n = super.getStoredLength();
            n += TypeSize.INTEGER;
            n += table.getStoredLength();
            return n;
        }

        @Override
        public void retrieve(ByteBuffer bb) {
            if (database == null) {
                throw new RSSException("Unexpected error: database is null");
            }
            super.retrieve(bb);
            actionId = bb.getInt();
            table = new TableDefinition(database);
            table.retrieve(bb);
        }

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

        public void setObjectFactory(ObjectRegistry objectFactory) {
            this.objectRegistry = objectFactory;
            database = (Database) objectRegistry.getInstance(DatabaseImpl.MODULE_ID);
        }
    }
}
