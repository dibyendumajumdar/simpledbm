package org.simpledbm.rss.main;

import java.util.Properties;

import org.simpledbm.rss.api.bm.BufferManager;
import org.simpledbm.rss.api.exception.RSSException;
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
import org.simpledbm.rss.api.tx.TransactionManager;
import org.simpledbm.rss.api.tx.TransactionalModuleRegistry;
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
import org.simpledbm.rss.impl.tx.TransactionManagerImpl;
import org.simpledbm.rss.impl.tx.TransactionalModuleRegistryImpl;
import org.simpledbm.rss.impl.wal.LogFactoryImpl;
import org.simpledbm.rss.util.logging.Logger;
import org.simpledbm.rss.util.mcat.MessageCatalog;

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
	public static final String LOCK_TABLE = "_internal/lock";
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
    
    final static MessageCatalog mcat = new MessageCatalog();
    StorageContainer lock;
    
    boolean started = false;
	
    private void assertNotStarted() {
    	if (started) {
    		throw new RSSException(mcat.getMessage("EV0003"));
    	}
    }

    private void assertStarted() {
    	if (!started) {
    		throw new RSSException(mcat.getMessage("EV0004"));
    	}
    }
 
	/**
	 * Creates a new RSS Server instance. An RSS Server instance contains at least a 
	 * LOG instance, and a VIRTUAL TABLE called dual. 
	 * @see LogFactory#createLog(Properties)
	 * @see LogFactory
	 */
	public static void create(Properties props) {
		Server server = new Server(props);
		final LogFactory logFactory = new LogFactoryImpl();
		logFactory.createLog(server.storageFactory, props);
		// SimpleDBM components expect a virtual container to exist with
		// a container ID of 0. This container must have at least one page.
		server.storageFactory.create(LOCK_TABLE).close();
		StorageContainer sc = server.storageFactory.create(VIRTUAL_TABLE);
		server.storageManager.register(VIRTUAL_TABLE_CONTAINER_ID, sc);
		Page page = server.pageFactory.getInstance(server.pageFactory.getRawPageType(), new PageId(VIRTUAL_TABLE_CONTAINER_ID, 0));
		server.pageFactory.store(page);
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
	 */
	public Server(Properties props) {

		String logProperties = props.getProperty("logging.properties.file", "classpath:logging.properties");
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
	 * @see LockManager#start()
	 * @see LogManager#start()
	 * @see BufferManager#start()
	 * @see TransactionManager#start()
	 */
	public synchronized void start() {
		assertNotStarted();
		lock = storageFactory.open(LOCK_TABLE);
		lock.lock();
		lockManager.start();
		logManager.start();
		bufferManager.start();
		transactionManager.start();
		log.info(LOG_CLASS_NAME, "start", mcat.getMessage("IV0001"));
		started = true;
	}
	
	/**
	 * Shuts down the RSS Server instance. This results in following actions.
	 * <ol>
	 * <li>The Transaction Manager is shutdown.</li>
	 * <li>The Buffer Manager is shutdown.</li>
	 * <li>The Log instance is shutdown.</li>
	 * <li>The Storage Manager instance is shutdown.</li>
	 * <li>The Lock Manager is shutdown.</li>
	 * </ol>
	 * 
	 * @see TransactionManager#shutdown()
	 * @see BufferManager#shutdown()
	 * @see LogManager#shutdown()
	 * @see StorageManager#shutdown()
	 */
	public synchronized void shutdown() {
		assertStarted();
		transactionManager.shutdown();
		bufferManager.shutdown();
		logManager.shutdown();
		storageManager.shutdown();
		lockManager.shutdown();
		lock.unlock();
		lock.close();
		log.info(LOG_CLASS_NAME, "shutdown", mcat.getMessage("IV0002"));
	}

	public synchronized  final IndexManager getIndexManager() {
		assertStarted();
		return indexManager;
	}

	public synchronized  final BufferManager getBufferManager() {
		assertStarted();
		return bufferManager;
	}

	public synchronized  final LatchFactory getLatchFactory() {
		assertStarted();
		return latchFactory;
	}

	public synchronized  final LockManager getLockManager() {
		assertStarted();
		return lockManager;
	}

	public synchronized  final LoggableFactory getLoggableFactory() {
		assertStarted();
		return loggableFactory;
	}

	public synchronized  final LogManager getLogManager() {
		assertStarted();
		return logManager;
	}

	public synchronized  final TransactionalModuleRegistry getModuleRegistry() {
		assertStarted();
		return moduleRegistry;
	}

	public synchronized  final ObjectRegistry getObjectRegistry() {
		assertStarted();
		return objectRegistry;
	}

	public synchronized  final PageFactory getPageFactory() {
		assertStarted();
		return pageFactory;
	}

	public synchronized  final FreeSpaceManager getSpaceManager() {
		assertStarted();
		return spaceManager;
	}

	public synchronized  final SlottedPageManager getSlottedPageManager() {
		assertStarted();
		return slottedPageManager;
	}

	public synchronized  final StorageContainerFactory getStorageFactory() {
		assertStarted();
		return storageFactory;
	}

	public  synchronized final StorageManager getStorageManager() {
		assertStarted();
		return storageManager;
	}

	public synchronized  final TransactionManager getTransactionManager() {
		assertStarted();
		return transactionManager;
	}

	public synchronized final TupleManager getTupleManager() {
		assertStarted();
		return tupleManager;
	}
}
