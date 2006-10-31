package org.simpledbm.rss.main;

import java.util.Properties;

import org.simpledbm.rss.api.bm.BufferManager;
import org.simpledbm.rss.api.fsm.FreeSpaceManager;
import org.simpledbm.rss.api.im.IndexManager;
import org.simpledbm.rss.api.latch.LatchFactory;
import org.simpledbm.rss.api.locking.LockManager;
import org.simpledbm.rss.api.locking.LockMgrFactory;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageFactory;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.registry.ObjectRegistry;
import org.simpledbm.rss.api.sp.SlottedPageManager;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.st.StorageManager;
import org.simpledbm.rss.api.tuple.TupleManager;
import org.simpledbm.rss.api.tx.LoggableFactory;
import org.simpledbm.rss.api.tx.TransactionalModuleRegistry;
import org.simpledbm.rss.api.tx.TransactionManager;
import org.simpledbm.rss.api.wal.LogFactory;
import org.simpledbm.rss.api.wal.LogManager;
import org.simpledbm.rss.impl.bm.BufferManagerImpl;
import org.simpledbm.rss.impl.fsm.FreeSpaceManagerImpl;
import org.simpledbm.rss.impl.im.btree.BTreeIndexManagerImpl;
import org.simpledbm.rss.impl.latch.LatchFactoryImpl;
import org.simpledbm.rss.impl.locking.LockManagerFactoryImpl;
import org.simpledbm.rss.impl.pm.PageFactoryImpl;
import org.simpledbm.rss.impl.registry.ObjectRegistryImpl;
import org.simpledbm.rss.impl.sp.SlottedPageManagerImpl;
import org.simpledbm.rss.impl.st.FileStorageContainerFactory;
import org.simpledbm.rss.impl.st.StorageManagerImpl;
import org.simpledbm.rss.impl.tuple.TupleManagerImpl;
import org.simpledbm.rss.impl.tx.LoggableFactoryImpl;
import org.simpledbm.rss.impl.tx.TransactionalModuleRegistryImpl;
import org.simpledbm.rss.impl.tx.TransactionManagerImpl;
import org.simpledbm.rss.impl.wal.LogFactoryImpl;
import org.simpledbm.rss.util.logging.Logger;

/**
 * A Server instance encapsulates all the modules that comprise the RSS. 
 * It ensures that all modules are initialized in the correct order and provides
 * a mechanism to start and stop the instance.
 * <p>
 * Note that the Server component acts very much like a custom IoC Container.  
 * 
 * @author Dibyendu Majumdar
 * @since 03-Apr-2006
 */
public class Server {

	private static String LOG_CLASS_NAME = Server.class.getName();
	private static Logger log = Logger.getLogger(Server.class.getPackage().getName());
	
	public static final String VIRTUAL_TABLE = "_internal/dual";
	public static final int VIRTUAL_TABLE_CONTAINER_ID = 0;
	
    final ObjectRegistry objectRegistry;
    final StorageContainerFactory storageFactory;
    final StorageManager storageManager;
    final LatchFactory latchFactory;
    final PageFactory pageFactory;
    final SlottedPageManager slottedPageManager;
    final LockManager lockManager;
    final LogManager logManager;
    final BufferManager bufferManager;
    final LoggableFactory loggableFactory;
    final TransactionalModuleRegistry moduleRegistry;
	final TransactionManager transactionManager;
    final FreeSpaceManager spaceManager;
    final IndexManager indexManager;
    final TupleManager tupleManager;
	
	/**
	 * Creates a new RSS Server instance. An RSS Server instance contains at least a 
	 * LOG instance, and a VIRTUAL TABLE called dual. 
	 * @see LogFactory#createLog(Properties)
	 * @see LogFactory
	 */
	public static void create(Properties props) throws Exception {
		Server server = new Server(props);
		final LogFactory logFactory = new LogFactoryImpl();
		logFactory.createLog(server.getStorageFactory(), props);
		// SimpleDBM components expect a virtual container to exist with
		// a container ID of 1. This container must have at least one page.
		StorageContainer sc = server.getStorageFactory().create(VIRTUAL_TABLE);
		server.getStorageManager().register(VIRTUAL_TABLE_CONTAINER_ID, sc);
		Page page = server.getPageFactory().getInstance(server.getPageFactory().getRawPageType(), new PageId(VIRTUAL_TABLE_CONTAINER_ID, 0));
		server.getPageFactory().store(page);
		// We start the server so that a checkpoint can be taken which will
		// ensure that the VIRTUAL_TABLE is automatically opened at system startup.
		server.start();
		server.shutdown();
	}

	/**
	 * Initializes a new RSS Server instance.
	 * <p>
	 * FIXME: Change exception specification.
	 * 
	 * @inheritDoc
	 * @see #start()
	 * @see #shutdown()
	 * @param props Properties that define various parameters for the system
	 * @throws Exception Thrown if there was a problem during initialization.
	 */
	public Server(Properties props) throws Exception {

		String logProperties = props.getProperty("logging.properties.file", "logging.properties");
		Logger.configure(logProperties);
		
		final LogFactory logFactory = new LogFactoryImpl();
		final LockMgrFactory lockMgrFactory = new LockManagerFactoryImpl();
		
		objectRegistry = new ObjectRegistryImpl();
		storageFactory = new FileStorageContainerFactory(props);
		storageManager = new StorageManagerImpl();
		latchFactory = new LatchFactoryImpl();
		pageFactory = new PageFactoryImpl(objectRegistry, storageManager, latchFactory);
		slottedPageManager = new SlottedPageManagerImpl(objectRegistry);
		loggableFactory = new LoggableFactoryImpl(objectRegistry);
		moduleRegistry = new TransactionalModuleRegistryImpl();
		lockManager = lockMgrFactory.create(props);
		logManager = logFactory.getLog(storageFactory, props);
		bufferManager = new BufferManagerImpl(logManager, pageFactory, props);
		transactionManager = new TransactionManagerImpl(logManager, storageFactory, storageManager, bufferManager, lockManager, loggableFactory, latchFactory, objectRegistry, moduleRegistry);
		spaceManager = new FreeSpaceManagerImpl(objectRegistry, pageFactory, logManager, bufferManager, storageManager, storageFactory, loggableFactory, transactionManager, moduleRegistry);
		indexManager = new BTreeIndexManagerImpl(objectRegistry, loggableFactory, spaceManager, bufferManager, slottedPageManager, moduleRegistry);
        tupleManager = new TupleManagerImpl(objectRegistry, loggableFactory, spaceManager, bufferManager, slottedPageManager, moduleRegistry, pageFactory);
	}

	/**
	 * Starts the Server instance. This results in following actions:
	 * <ol>
	 * <li>The Log instance is opened. This starts the background threads that manage log writes.</li>
	 * <li>The Buffer Manager instance is started. This starts up the Buffer Writer thread.</li>
	 * <li>The Transaction Manager is started. This initiates restart recovery, and also starts the
	 *     Checkpoint thread.</li>
	 * </ol>
	 * <p>
	 * FIXME: Exception signature needs to be fixed.
	 * 
	 * @see LogManager#start()
	 * @see BufferManager#start()
	 * @see TransactionManager#start()
	 * @throws Exception Thrown if there was a problem starting the Server instance.
	 */
	public void start() throws Exception {
		getLogManager().start();
		getBufferManager().start();
		getTransactionManager().start();
		log.info(LOG_CLASS_NAME, "start", "SIMPLEDBM-LOG: RSS Server started.");
	}
	
	/**
	 * Shuts down the RSS Server instance. This results in following actions.
	 * <ol>
	 * <li>The Transaction Manager is shutdown.</li>
	 * <li>The Buffer Manager is shutdown.</li>
	 * <li>The Log instance is shutdown.</li>
	 * <li>The Storage Manager instance is shutdown.</li>
	 * </ol>
	 * 
	 * @see TransactionManager#shutdown()
	 * @see BufferManager#shutdown()
	 * @see LogManager#shutdown()
	 * @see StorageManager#shutdown()
	 */
	public void shutdown() {
		getTransactionManager().shutdown();
		getBufferManager().shutdown();
		getLogManager().shutdown();
		getStorageManager().shutdown();
		log.info(LOG_CLASS_NAME, "shutdown", "SIMPLEDBM-LOG: RSS Server shutdown completed.");
	}

	public final IndexManager getIndexManager() {
		return indexManager;
	}

	public final BufferManager getBufferManager() {
		return bufferManager;
	}

	public final LatchFactory getLatchFactory() {
		return latchFactory;
	}

	public final LockManager getLockManager() {
		return lockManager;
	}

	public final LoggableFactory getLoggableFactory() {
		return loggableFactory;
	}

	public final LogManager getLogManager() {
		return logManager;
	}

	public final TransactionalModuleRegistry getModuleRegistry() {
		return moduleRegistry;
	}

	public final ObjectRegistry getObjectRegistry() {
		return objectRegistry;
	}

	public final PageFactory getPageFactory() {
		return pageFactory;
	}

	public final FreeSpaceManager getSpaceManager() {
		return spaceManager;
	}

	public final SlottedPageManager getSlottedPageManager() {
		return slottedPageManager;
	}

	public final StorageContainerFactory getStorageFactory() {
		return storageFactory;
	}

	public final StorageManager getStorageManager() {
		return storageManager;
	}

	public final TransactionManager getTransactionManager() {
		return transactionManager;
	}

	public final TupleManager getTupleManager() {
		return tupleManager;
	}
}
