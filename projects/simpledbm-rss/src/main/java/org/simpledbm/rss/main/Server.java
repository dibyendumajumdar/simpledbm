/**
 * DO NOT REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Contributor(s):
 *
 * The Original Software is SimpleDBM (www.simpledbm.org).
 * The Initial Developer of the Original Software is Dibyendu Majumdar.
 *
 * Portions Copyright 2005-2014 Dibyendu Majumdar. All Rights Reserved.
 *
 * The contents of this file are subject to the terms of the
 * Apache License Version 2 (the "APL"). You may not use this
 * file except in compliance with the License. A copy of the
 * APL may be obtained from:
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the APL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the APL, the GPL or the LGPL.
 *
 * Copies of GPL and LGPL may be obtained from:
 * http://www.gnu.org/licenses/license-list.html
 */
package org.simpledbm.rss.main;

import java.util.Properties;

import org.simpledbm.common.api.exception.ExceptionHandler;
import org.simpledbm.common.api.exception.SimpleDBMException;
import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.api.registry.ObjectRegistry;
import org.simpledbm.common.api.tx.IsolationMode;
import org.simpledbm.common.impl.platform.PlatformImpl;
import org.simpledbm.common.impl.registry.ObjectRegistryImpl;
import org.simpledbm.common.util.logging.Logger;
import org.simpledbm.common.util.mcat.Message;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.common.util.mcat.MessageType;
import org.simpledbm.rss.api.bm.BufferManager;
import org.simpledbm.rss.api.fsm.FreeSpaceManager;
import org.simpledbm.rss.api.im.IndexContainer;
import org.simpledbm.rss.api.im.IndexManager;
import org.simpledbm.rss.api.latch.LatchFactory;
import org.simpledbm.rss.api.loc.LocationFactory;
import org.simpledbm.rss.api.locking.LockManager;
import org.simpledbm.rss.api.locking.LockMgrFactory;
import org.simpledbm.rss.api.locking.util.LockAdaptor;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.pm.PageManager;
import org.simpledbm.rss.api.sp.SlottedPageManager;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.st.StorageException;
import org.simpledbm.rss.api.st.StorageManager;
import org.simpledbm.rss.api.tuple.TupleContainer;
import org.simpledbm.rss.api.tuple.TupleManager;
import org.simpledbm.rss.api.tx.LoggableFactory;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.rss.api.tx.TransactionManager;
import org.simpledbm.rss.api.tx.TransactionalModuleRegistry;
import org.simpledbm.rss.api.wal.LogFactory;
import org.simpledbm.rss.api.wal.LogManager;
import org.simpledbm.rss.impl.bm.BufferManagerImpl;
import org.simpledbm.rss.impl.fsm.FreeSpaceManagerImpl;
import org.simpledbm.rss.impl.im.btree.BTreeIndexManagerImpl;
import org.simpledbm.rss.impl.latch.LatchFactoryImpl;
import org.simpledbm.rss.impl.locking.LockManagerFactoryImpl;
import org.simpledbm.rss.impl.locking.util.DefaultLockAdaptor;
import org.simpledbm.rss.impl.pm.PageManagerImpl;
import org.simpledbm.rss.impl.sp.SlottedPageManagerImpl;
import org.simpledbm.rss.impl.st.FileStorageContainerFactory;
import org.simpledbm.rss.impl.st.StorageManagerImpl;
import org.simpledbm.rss.impl.tuple.TupleManagerImpl;
import org.simpledbm.rss.impl.tx.LoggableFactoryImpl;
import org.simpledbm.rss.impl.tx.TransactionManagerImpl;
import org.simpledbm.rss.impl.tx.TransactionalModuleRegistryImpl;
import org.simpledbm.rss.impl.wal.LogFactoryImpl;

/**
 * A Server instance encapsulates all the modules that comprise the RSS. It
 * ensures that all modules are initialized in the correct order and provides a
 * mechanism to start and stop the instance.
 * <p>
 * Note that the Server component acts very much like a custom IoC Container.
 * 
 * @author Dibyendu Majumdar
 * @since 03-Apr-2006
 */
public class Server {

    public static final String LOGGER_NAME = "org.simpledbm.server";

    final Logger log;
    final ExceptionHandler exceptionHandler;

    private static final String VIRTUAL_TABLE = "_internal/dual";
    private static final String LOCK_TABLE = "_internal/lock";
    private static final int VIRTUAL_TABLE_CONTAINER_ID = 0;

    final private Platform platform;
    final private ObjectRegistry objectRegistry;
    final private StorageContainerFactory storageFactory;
    final private StorageManager storageManager;
    final private LatchFactory latchFactory;
    final private PageManager pageFactory;
    final private SlottedPageManager slottedPageManager;
    final private LockManager lockManager;
    final private LogManager logManager;
    final private BufferManager bufferManager;
    final private LoggableFactory loggableFactory;
    final private TransactionalModuleRegistry moduleRegistry;
    final private TransactionManager transactionManager;
    final private FreeSpaceManager spaceManager;
    final private IndexManager indexManager;
    final private TupleManager tupleManager;
    final private boolean platformAllocated;

    private StorageContainer lock;

    private boolean started = false;

    // Server messages
    static Message m_IV0001 = new Message('R', 'V', MessageType.INFO, 1,
            "SimpleDBM RSS Server STARTED");
    static Message m_IV0002 = new Message('R', 'V', MessageType.INFO, 2,
            "SimpleDBM RSS Server STOPPED");
    static Message m_EV0003 = new Message('R', 'V', MessageType.ERROR, 3,
            "SimpleDBM RSS Server cannot be started more than once");
    static Message m_EV0004 = new Message('R', 'V', MessageType.ERROR, 4,
            "SimpleDBM RSS Server has not been started");
    static Message m_EV0005 = new Message(
            'R',
            'V',
            MessageType.ERROR,
            5,
            "Error starting SimpleDBM RSS Server, another instance may be running - error was: {0}");

    private void assertNotStarted() {
        if (started) {
            exceptionHandler.errorThrow(this.getClass(),
                    "assertNotStarted", new SimpleDBMException(
                            new MessageInstance(m_EV0003)));
        }
    }

    private void assertStarted() {
        if (!started) {
            exceptionHandler.errorThrow(this.getClass(),
                    "assertNotStarted", new SimpleDBMException(
                            new MessageInstance(m_EV0004)));
        }
    }

    /**
     * Attempts to exclsively lock the Server instance.
     */
    private void lockServerInstance() {
        boolean lockObtained = false;
        /*
         * We have to carefully handle locks - so that
         * a) We do not overwrite the exiting lock object in case server has been erroneously started second time
         * b) We do not leave file handle open if the lock fails
         */
        StorageContainer lock = null;
        try {
            /*
             * Perform atomic creation of lock container.
             */
            lock = storageFactory.createIfNotExisting(LOCK_TABLE);
            /*
             * Now also lock this container.
             */
            lock.lock();
            lockObtained = true;
        } catch (StorageException e) {
            exceptionHandler.errorThrow(getClass(), "start",
                    new SimpleDBMException(new MessageInstance(m_EV0005, e
                            .getMessage()), e));
        } finally {
            if (!lockObtained) {
                if (lock != null) {
                    lock.close();
                }
            } else {
                this.lock = lock;
            }
        }
    }

    /**
     * Unlocks the Server instance lock.
     */
    private void unlockServerInstance() {
        lock.unlock();
        lock.close();
        storageFactory.delete(LOCK_TABLE);
    }

    /**
     * Creates a new RSS Server instance. An RSS Server instance contains at
     * least a LOG instance, two virtual tables - dual and lock. Note that this
     * will overwrite any existing database on the same path, hence caller needs
     * to be sure that the intention is to create a new database.
     * 
     * @see LogFactory#createLog()
     * @see LogFactory
     */
    public static void create(Properties props) {
        // TODO Need to ensure that we do not overwrite an existing database without warning
        Server server = new Server(props);
        final LogFactory logFactory = new LogFactoryImpl(server.platform, server.storageFactory, props);
        server.lockServerInstance();
        logFactory.createLog();
        // SimpleDBM components expect a virtual container to exist with
        // a container ID of 0. This container must have at least one page.
        // server.storageFactory.create(LOCK_TABLE).close();
        StorageContainer sc = server.storageFactory.create(VIRTUAL_TABLE);
        server.storageManager.register(VIRTUAL_TABLE_CONTAINER_ID, sc);
        Page page = server.pageFactory.getInstance(server.pageFactory
                .getRawPageType(), new PageId(VIRTUAL_TABLE_CONTAINER_ID, 0));
        server.pageFactory.store(page);
        server.unlockServerInstance();
        // We start the server so that a checkpoint can be taken which will
        // ensure that the VIRTUAL_TABLE is automatically opened at system startup.
        server.start();
        server.shutdown();
    }

    /**
     * Drops a database instance.
     */
    public static void drop(Properties props) {
        Server server = new Server(props);
        server.lockServerInstance();
        server.unlockServerInstance();
        /*
         * There is a potential race condition here because
         * between the gap that exists here someone else could
         * start the server.
         * FIXME
         */
        server.getStorageFactory().drop();
    }

    public Server(Platform platform, Properties props) {

        if (platform == null) {
            platform = new PlatformImpl(props);
            platformAllocated = true;
        }
        else {
            platformAllocated = false;
        }
        this.platform = platform;
        PlatformObjects po = platform.getPlatformObjects(Server.LOGGER_NAME);
        log = po.getLogger();
        exceptionHandler = po.getExceptionHandler();

        storageFactory = new FileStorageContainerFactory(platform, props);

        final LogFactory logFactory = new LogFactoryImpl(platform, storageFactory, props);
        final LockMgrFactory lockMgrFactory = new LockManagerFactoryImpl(
                platform, props);

        LockAdaptor lockAdaptor = new DefaultLockAdaptor(platform, props);
        objectRegistry = new ObjectRegistryImpl(platform, props);
        storageManager = new StorageManagerImpl(platform, props);
        latchFactory = new LatchFactoryImpl(platform, props);
        pageFactory = new PageManagerImpl(platform, objectRegistry,
                storageManager, latchFactory, props);
        slottedPageManager = new SlottedPageManagerImpl(platform,
                objectRegistry, pageFactory, props);
        loggableFactory = new LoggableFactoryImpl(platform, objectRegistry,
                props);
        moduleRegistry = new TransactionalModuleRegistryImpl(platform, props);
        lockManager = lockMgrFactory.create(latchFactory, props);
        logManager = logFactory.getLog();
        bufferManager = new BufferManagerImpl(platform, logManager,
                pageFactory, props);
        transactionManager = new TransactionManagerImpl(platform, logManager,
                storageFactory, storageManager, bufferManager, lockManager,
                loggableFactory, latchFactory, objectRegistry, moduleRegistry,
                props);
        spaceManager = new FreeSpaceManagerImpl(platform, objectRegistry,
                pageFactory, logManager, bufferManager, storageManager,
                storageFactory, loggableFactory, transactionManager,
                moduleRegistry, props);
        indexManager = new BTreeIndexManagerImpl(platform, objectRegistry,
                loggableFactory, spaceManager, bufferManager,
                slottedPageManager, moduleRegistry, lockAdaptor, props);
        tupleManager = new TupleManagerImpl(platform, objectRegistry,
                loggableFactory, spaceManager, bufferManager,
                slottedPageManager, moduleRegistry, pageFactory, lockAdaptor,
                props);
    }

    /**
     * Initializes a new RSS Server instance.
     * 
     * @see #start()
     * @see #shutdown()
     * @param props Properties that define various parameters for the system
     */
    public Server(Properties props) {
        this(null, props);
    }

    /**
     * Starts the Server instance. This results in following actions:
     * <ol>
     * <li>The Lock Manager is started. This enables background thread for
     * deadlock detection.</li>
     * <li>The Log instance is opened. This starts the background threads that
     * manage log writes and log archiving.</li>
     * <li>The Buffer Manager instance is started. This starts up the background
     * Buffer Writer thread.</li>
     * <li>The Transaction Manager is started. This initiates restart recovery,
     * and also starts the Checkpoint thread.</li>
     * </ol>
     * <p>
     * To prevent two server instances running concurrently on the same path, a
     * lock file is used. If a server is already running on the specified path,
     * the start() will fail with an exception.
     * 
     * @see LockManager#start()
     * @see LogManager#start()
     * @see BufferManager#start()
     * @see TransactionManager#start()
     */
    public synchronized void start() {
        assertNotStarted();
        lockServerInstance();
        lockManager.start();
        logManager.start();
        bufferManager.start();
        transactionManager.start();
        log.info(getClass(), "start", new MessageInstance(m_IV0001)
                .toString());
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

        // Trace.dump();

        transactionManager.shutdown();
        bufferManager.shutdown();
        logManager.shutdown();
        storageManager.shutdown();
        lockManager.shutdown();
        if (platformAllocated) {
            platform.shutdown();
        }
        unlockServerInstance();
        log.info(getClass(), "shutdown",
                new MessageInstance(m_IV0002).toString());
    }

    public synchronized final IndexManager getIndexManager() {
        return indexManager;
    }

    public synchronized final BufferManager getBufferManager() {
        return bufferManager;
    }

    public synchronized final LatchFactory getLatchFactory() {
        return latchFactory;
    }

    public synchronized final LockManager getLockManager() {
        return lockManager;
    }

    public synchronized final LoggableFactory getLoggableFactory() {
        return loggableFactory;
    }

    public synchronized final LogManager getLogManager() {
        return logManager;
    }

    public synchronized final TransactionalModuleRegistry getModuleRegistry() {
        return moduleRegistry;
    }

    public synchronized final ObjectRegistry getObjectRegistry() {
        return objectRegistry;
    }

    public synchronized final PageManager getPageFactory() {
        return pageFactory;
    }

    public synchronized final FreeSpaceManager getSpaceManager() {
        return spaceManager;
    }

    public synchronized final SlottedPageManager getSlottedPageManager() {
        return slottedPageManager;
    }

    public synchronized final StorageContainerFactory getStorageFactory() {
        return storageFactory;
    }

    public synchronized final StorageManager getStorageManager() {
        return storageManager;
    }

    public synchronized final TransactionManager getTransactionManager() {
        return transactionManager;
    }

    public synchronized final TupleManager getTupleManager() {
        return tupleManager;
    }

    public synchronized final LocationFactory getLocationFactory() {
        return getTupleManager().getLocationFactory();
    }

    public synchronized final int getLocationFactoryType() {
        return getTupleManager().getLocationFactoryType();
    }

    /**
     * Creates a new index with specified container name and ID. Prior to
     * calling this method, an exclusive lock should be obtained on the
     * container ID to ensure that no other transaction is simultaneously
     * attempting to access the same container. If successful, by the end of
     * this call, the container should have been created and registered with the
     * StorageManager, and an empty instance of the index created within the
     * container.
     * 
     * @param trx Transaction managing the creation of the index
     * @param name Name of the container
     * @param containerId ID of the new container, must be unused
     * @param extentSize Number of pages in each extent of the container
     * @param keyFactoryType Identifies the factory for creating IndexKey
     *            objects
     * @param unique If true, the new index will not allow duplicates keys
     */
    public void createIndex(Transaction trx, String name, int containerId,
            int extentSize, int keyFactoryType, boolean unique) {
        getIndexManager().createIndex(trx, name, containerId, extentSize,
                keyFactoryType, getTupleManager().getLocationFactoryType(),
                unique);
    }

    /**
     * Creates a new index with specified container name and ID. Prior to
     * calling this method, an exclusive lock should be obtained on the
     * container ID to ensure that no other transaction is simultaneously
     * attempting to access the same container. If successful, by the end of
     * this call, the container should have been created and registered with the
     * StorageManager, and an empty instance of the index created within the
     * container.
     * 
     * @param trx Transaction managing the creation of the index
     * @param name Name of the container
     * @param containerId ID of the new container, must be unused
     * @param extentSize Number of pages in each extent of the container
     * @param keyFactoryType Identifies the factory for creating IndexKey
     *            objects
     * @param locationFactoryType Identifies the factory for creating Location
     *            objects
     * @param unique If true, the new index will not allow duplicates keys
     */
    public void createIndex(Transaction trx, String name, int containerId,
            int extentSize, int keyFactoryType, int locationFactoryType,
            boolean unique) {
        indexManager.createIndex(trx, name, containerId, extentSize,
                keyFactoryType, locationFactoryType, unique);
    }

    /**
     * Obtains an existing index with specified container ID. A Shared lock is
     * obtained on the container ID to ensure that no other transaction is
     * simultaneously attempting to create/delete the same container.
     * 
     * @param containerId ID of the container, must have been initialized as an
     *            Index prior to this call
     */
    public IndexContainer getIndex(Transaction trx, int containerId) {
        return getIndexManager().getIndex(trx, containerId);
    }

    /**
     * Registers a Singleton object to the registry.
     * 
     * @param typecode A unique type code for the type.
     * @param object The object to be registered.
     */
    public void registerSingleton(int typecode, Object object) {
        getObjectRegistry().registerSingleton(typecode, object);
    }

    /**
     * Registers a class to the Object Registry. The class must implement a
     * no-arg constructor. The class may optionally implement
     * {@link ObjectRegistryAware} interface.
     * 
     * @param typecode A unique type code for the type.
     * @param classname The class name.
     */
    //    public void registerType(int typecode, String classname) {
    //        getObjectRegistry().registerType(typecode, classname);
    //    }

    /**
     * Begins a new transaction.
     */
    public Transaction begin(IsolationMode isolationMode) {
        return getTransactionManager().begin(isolationMode);
    }

    /**
     * Creates a new Tuple Container.
     * 
     * @param trx Transaction to be used for creating the container
     * @param name Name of the container
     * @param containerId A numeric ID for the container - must be unique for
     *            each container
     * @param extentSize The number of pages that should be part of each extent
     *            in the container
     */
    public void createTupleContainer(Transaction trx, String name,
            int containerId, int extentSize) {
        getTupleManager().createTupleContainer(trx, name, containerId,
                extentSize);
    }

    /**
     * Gets an instance of TupleContainer. Specified container must already
     * exist. Obtains SHARED lock on specified containerId.
     * 
     * @param containerId ID of the container
     */
    public TupleContainer getTupleContainer(Transaction trx, int containerId) {
        return getTupleManager().getTupleContainer(trx, containerId);
    }
}
