.. -*- coding: utf-8 -*-

===========================
SimpleDBM RSS User's Manual
===========================

:Author: Dibyendu Majumdar
:Contact: d.majumdar@gmail.com
:Version: 1.0.12
:Date: 7 April 2009
:Copyright: Copyright by Dibyendu Majumdar, 2007-2009

.. contents::

------------
Introduction
------------

Overview
========

SimpleDBM_ is a transactional database engine, written in Java. It has a
very small footprint and can be embedded in the address space of an
application. It provides a simple Java application programming interface (API), 
which can be learned very quickly.

.. _SimpleDBM: http://www.simpledbm.org

Features
========

SimpleDBM has the following features:

- *Transactional* - SimpleDBM fully supports ACID transactions. A STEAL and NO-FORCE buffer management strategy is used for transactions which is optimum for performance.
- *Multi-threaded* - SimpleDBM is multi-threaded and supports concurrent reads and writes of data.
- *Write Ahead Log* - SimpleDBM uses a write ahead log to ensure transaction recovery in the event of system crashes.
- *Lock based concurrency* - SimpleDBM uses row-level shared, update and exclusive locks to manage concurrency. 
- *Multiple Isolation Levels* - SimpleDBM supports read committed, repeatable read, and serializable isolation levels.
- *B-Tree Indexes* - SimpleDBM implements B-plus Tree indexes, that fully support concurrent reads, inserts and deletes. SimpleDBM B-Trees continually rebalance themselves, and do not suffer from fragmentation.
- *Tables* - SimpleDBM supports tables, but for maximum flexibility, treats table rows as blobs of data. Table rows can have any internal structure as you like, and can span multiple disk pages.
- *Latches and Locks* - SimpleDBM uses latches for internal consistency, and locks for concurrency. Latches are more efficient locking mechanisms that do not suffer from deadlocks.
- *Deadlock detection* - SimpleDBM has support for deadlock detection. A background thread periodically checks the lock table for deadlocks and aborts transactions to resolve deadlocks.

Non-Features
------------
- SimpleDBM is not an SQL engine. 
- There is no support for distributed transactions (XA) yet.

Status
------

SimpleDBM is currently in early BETA and not suitable for Production use. Note that the simpleDBM API is under flux, and is likely to change until the final 1.0 release is available. 

The latest builds can be downloaded from:

http://code.google.com/p/simpledbm/downloads/list.

Getting Started
---------------

Download the latest build of SimpleDBM.
There are no special requirements or third party library dependencies; all you
need to ensure is that the SimpleDBM jar file is in your classpath.

SimpleDBM does not come with a type system of its own, but there is a sample
type system implementation available, which you can enhance. 

-----------------
SimpleDBM Modules
-----------------

The core of SimpleDBM_ is the RSS (named in honor of the
first IBM Relational Database prototype `System-R <http://www.mcjones.org/System_R/>`_ Relational Storage
System). The RSS provides the underlying storage structures for
transactions, locking, b-trees etc. 

This document covers the RSS API.

Note that the RSS API is probably too low level for ordinary users. 
It is meant to be used by people interested
in build their own Database Engines on top. The design of the RSS is described in 
detail in the `SimpleDBM RSS Developers's Guide <http://www.simpledbm.org>`_.

For users looking for a simpler API, two additional modules are
available. 

The first one is the SimpleDBM_ TypeSystem module, which adds support
for typed data values and multi-attribute row objects. For details of this
module, please read `SimpleDBM TypeSystem <http://www.simpledbm.org>`_.

The second module, the Database API, implements a high level 
Database API and uses the TypeSystem module on top of the RSS. For details of this
module, please read `SimpleDBM Database API <http://www.simpledbm.org>`_.

-------------------------------
SimpleDBM Servers and Databases
-------------------------------

Introduction
============

A SimpleDBM server is a set of background threads and a library of API
calls that clients can hook into. The background threads take care of
various tasks, such as writing out buffer pages, writing out logs,
archiving older log files, creating checkpoints, etc.

A SimpleDBM server operates on a set of data and index files, known as
the SimpleDBM database.

Only one server instance is allowed to access a SimpleDBM database at
any point in time. SimpleDBM uses a lock file to detect multiple
concurrent access to a database, and will refuse to start if it
detects that a server is already accessing a database.

Internally, SimpleDBM operates on logical entities called Storage
Containers. From an implementation point of view, Storage Containers
are mapped to files. 

Tables and Indexes are stored in Containers known as TupleContainers
and IndexContainers, respectively.

The SimpleDBM database initially consists of a set of transaction log
files, a lock file and a special container used internally by
SimpleDBM.

Creating a SimpleDBM database
=============================

A SimpleDBM database is created by a call to Server.create(), as shown
below: ::

  import org.simpledbm.rss.main.Server;
  ...  
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
  properties.setProperty("storage.basePath", 
    "demodata/TupleDemo1");
  
  Server.create(properties);

The Server.create() method accepts a Properties object as
the sole argument. The Properties object can be used to pass a
number of parameters. The available options are shown below:

Server Options
--------------

+-------------------------------------+------------------------------------------------------------+
| Property Name                       | Description                                                |
+=====================================+============================================================+
| ``log.ctl.{n}``                     | The fully qualified path to the                            |
|                                     | log control file. The first file should be specified as    |
|                                     | ``log.ctl.1``, second as ``log.ctl.2``, and so on. Up to a |
|                                     | maximum of 3 can be specified. Default is 2.               |
+-------------------------------------+------------------------------------------------------------+
| ``log.groups.{n}.path``             | The path where log files of a group should be stored.      |
|                                     | The first log group is specified as ``log.groups.1.path``, |
|                                     | the second as ``log.groups.2.path``,                       |
|                                     | and so on. Up to a maximum of 3 log groups can be          |
|                                     | specified. Default number of groups is 1. Path defaults    |
|                                     | to current directory.                                      |
+-------------------------------------+------------------------------------------------------------+
| ``log.archive.path``                | Defines the path for storing archive files. Defaults to    | 
|                                     | current directory.                                         |
+-------------------------------------+------------------------------------------------------------+
| ``log.group.files``                 | Specifies the number of log files within each group.       |
|                                     | Up to a maximum of 8 are allowed. Defaults to 2.           |
+-------------------------------------+------------------------------------------------------------+
| ``log.file.size``                   | Specifies the size of each log file in                     |
|                                     | bytes. Default is 2 KB.                                    |
+-------------------------------------+------------------------------------------------------------+
| ``log.buffer.size``                 | Specifies the size of the log buffer                       |
|                                     | in bytes. Default is 2 KB.                                 |
+-------------------------------------+------------------------------------------------------------+
| ``log.buffer.limit``                | Sets a limit on the maximum number of                      |
|                                     | log buffers that can be allocated. Default is 10 *         |
|                                     | log.group.files.                                           |
+-------------------------------------+------------------------------------------------------------+
| ``log.flush.interval``              | Sets the interval (in seconds)                             |
|                                     | between log flushes. Default is 6 seconds.                 |
+-------------------------------------+------------------------------------------------------------+
| ``log.disableFlushRequests``        | Boolean value, if set, disables                            |
|                                     | log flushes requested explicitly by the Buffer Manager     |
|                                     | or Transaction Manager. Log flushes still occur during     |
|                                     | checkpoints and log switches. By reducing the log flushes, |
|                                     | performance is improved, but transactions may not be       |
|                                     | durable. Only those transactions will survive a system     | 
|                                     | crash that have all their log records on disk.             |
+-------------------------------------+------------------------------------------------------------+
| ``storage.basePath``                | Defines the base location of the                           |
|                                     | SimpleDBM database. All files and directories are created  |
|                                     | relative to this location.                                 |
+-------------------------------------+------------------------------------------------------------+
| ``storage.createMode``              | Defines mode in which files will be                        |
|                                     | created. Default is ``"rws"``.                             |
+-------------------------------------+------------------------------------------------------------+
| ``storage.openMode``                | Defines mode in which files will be                        |
|                                     | opened. Default is ``"rws"``.                              |
+-------------------------------------+------------------------------------------------------------+
| ``storage.flushMode``               | Defines mode in which files will be flushed. Possible      |
|                                     | values are noforce, force.true (default), and force.false  |
+-------------------------------------+------------------------------------------------------------+
| ``bufferpool.numbuffers``           | Sets the number of buffers to be created in                |
|                                     | the Buffer Pool.                                           |
+-------------------------------------+------------------------------------------------------------+
| ``bufferpool.writerSleepInterval``  | Sets the interval in milliseconds between each run of      |
|                                     | the BufferWriter. Note that BufferWriter may run earlier   |
|                                     | than the specified interval if the pool runs out of        |
|                                     | buffers, and a new page has to be read in. In such cases,  |
|                                     | the Buffer Writer may be manually triggered to clean out   |
|                                     | buffers.                                                   |
+-------------------------------------+------------------------------------------------------------+
| ``lock.deadlock.detection.interval``| Sets the interval in seconds between deadlock scans.       |
+-------------------------------------+------------------------------------------------------------+
| ``logging.properties.file``         | Specifies the name of logging properties file. Precede     |
|                                     | ``classpath:`` if you want SimpleDBM to search for this    |
|                                     | file in the classpath.                                     |
+-------------------------------------+------------------------------------------------------------+
| ``logging.properties.type``         | Specify ``"log4j"`` if you want to SimpleDBM to use Log4J  |
|                                     | for generating log messages.                               |
+-------------------------------------+------------------------------------------------------------+
| ``transaction.lock.timeout``        | Specifies the default lock timeout value in seconds.       |
|                                     | Default is 60 seconds.                                     |
+-------------------------------------+------------------------------------------------------------+
| ``transaction.ckpt.interval``       | Specifies the interval between checkpoints in milliseconds.|
|                                     | Default is 15000 milliseconds (15 secs).                   |
+-------------------------------------+------------------------------------------------------------+

The ``Server.create()`` call will overwrite any existing database
in the specified storage path, so it must be called only when you know
for sure that you want to create a database.

Opening a database
==================

Once a database has been created, it can be opened by creating an
instance of SimpleDBM server, and starting it. The same properties that were
supplied while creating the database, can be supplied when starting it.

Here is a code snippet that shows how this is done: ::

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
  properties.setProperty("storage.basePath", 
    "demodata/TupleDemo1");

  Server server = new Server(properties);
  server.start();
  try {
    // do some work
  }
  finally {
    server.shutdown();
  }

Some points to bear in mind when starting SimpleDBM server instances:

1. Make sure that you invoke ``shutdown()`` eventually to ensure proper
   shutdown of the database.
2. Database startup/shutdown is relatively expensive, so do it only
   once during the life-cycle of your application.
3. A Server object can be used only once - after calling
   ``shutdown()``, it is an error to do any operation with the server
   object.

Managing log messages
=====================

SimpleDBM has support for JDK 1.4 style logging as well as
Log4J logging. By default, if Log4J library is available on the
classpath, SimpleDBM will use it. Otherwise, JDK 1.4 util.logging
package is used.

You can specify the type of logging to be used using the
Server Property ``logging.properties.type``. If this is set to
"log4j", SimpleDBM will use Log4J logging. Any other value causes
SimpleDBM to use default JDK logging.

The configuration of the logging can be specified using a 
properties file. The name and location of the properties file
is specified using the Server property ``logging.properties.file``.
If the filename is prefixed with the string "classpath:", then
SimpleDBM will search for the properties file in the classpath. 
Otherwise, the filename is searched for in the current filesystem.

A sample logging properties file is shown below. Note that this
sample contains both JDK style and Log4J style configuration.::

 ############################################################
 #  	JDK 1.4 Logging
 ############################################################
 handlers= java.util.logging.FileHandler, java.util.logging.ConsoleHandler
 .level= INFO

 java.util.logging.FileHandler.pattern = simpledbm.log.%g
 java.util.logging.FileHandler.limit = 50000
 java.util.logging.FileHandler.count = 1
 java.util.logging.FileHandler.formatter = java.util.logging.SimpleFormatter
 java.util.logging.FileHandler.level = ALL

 java.util.logging.ConsoleHandler.formatter = java.util.logging.SimpleFormatter
 java.util.logging.ConsoleHandler.level = ALL

 org.simpledbm.registry.level = INFO
 org.simpledbm.bufmgr.level = INFO
 org.simpledbm.indexmgr.level = INFO
 org.simpledbm.storagemgr.level = INFO
 org.simpledbm.walogmgr.level = INFO
 org.simpledbm.lockmgr.level = INFO
 org.simpledbm.freespacemgr.level = INFO
 org.simpledbm.slotpagemgr.level = INFO
 org.simpledbm.transactionmgr.level = INFO
 org.simpledbm.tuplemgr.level = INFO
 org.simpledbm.latchmgr.level = INFO
 org.simpledbm.pagemgr.level = INFO
 org.simpledbm.rss.util.level = INFO
 org.simpledbm.util.level = INFO
 org.simpledbm.server.level = INFO
 org.simpledbm.trace.level = INFO
 org.simpledbm.database.level = INFO

 # Default Log4J configuration

 # Console appender
 log4j.appender.A1=org.apache.log4j.ConsoleAppender
 log4j.appender.A1.layout=org.apache.log4j.PatternLayout
 log4j.appender.A1.layout.ConversionPattern=%d [%t] %p %c %m%n

 # File Appender
 log4j.appender.A2=org.apache.log4j.RollingFileAppender
 log4j.appender.A2.MaxFileSize=10MB
 log4j.appender.A2.MaxBackupIndex=1
 log4j.appender.A2.File=simpledbm.log
 log4j.appender.A2.layout=org.apache.log4j.PatternLayout
 log4j.appender.A2.layout.ConversionPattern=%d [%t] %p %c %m%n

 # Root logger set to DEBUG using the A1 and A2 appenders defined above.
 log4j.rootLogger=DEBUG, A1, A2

 # Various loggers
 log4j.logger.org.simpledbm.registry=INFO
 log4j.logger.org.simpledbm.bufmgr=INFO
 log4j.logger.org.simpledbm.indexmgr=INFO
 log4j.logger.org.simpledbm.storagemgr=INFO
 log4j.logger.org.simpledbm.walogmgr=INFO
 log4j.logger.org.simpledbm.lockmgr=INFO
 log4j.logger.org.simpledbm.freespacemgr=INFO
 log4j.logger.org.simpledbm.slotpagemgr=INFO
 log4j.logger.org.simpledbm.transactionmgr=INFO
 log4j.logger.org.simpledbm.tuplemgr=INFO
 log4j.logger.org.simpledbm.latchmgr=INFO
 log4j.logger.org.simpledbm.pagemgr=INFO
 log4j.logger.org.simpledbm.rss.util=INFO
 log4j.logger.org.simpledbm.util=INFO
 log4j.logger.org.simpledbm.server=INFO
 log4j.logger.org.simpledbm.trace=INFO
 log4j.logger.org.simpledbm.database=INFO

By default, SimpleDBM looks for a logging properties file named
"simpledbm.logging.properties".


---------------------
Common Infrastructure
---------------------

Object Registry
===============

Overview
--------
SimpleDBM uses a custom serialization mechanism for marshalling
and unmarshalling objects to/from byte streams. When an object 
is marshalled, a two-byte code is stored in the first two bytes of 
the byte stream [1]_. This identifies the class of the stored object.
When reading back, the type code is used to lookup a factory for
creating the object of the correct class.

.. [1] In some cases the type code is not stored, as it can be
       determined through some other means. 

The SimpleDBM serialization mechanism does not use the Java
Serialization framework.

Central to the SimpleDBM serialization mechanism is the ``ObjectRegistry``
module. The ``ObjectRegistry`` provides the coupling between SimpleDBM 
serialization mechanism and its clients. For instance, index key
types, table row types, etc. are registered in SimpleDBM's
``ObjectRegistry`` and thereby made available to SimpleDBM. You will
see how this is done when we discuss Tables and 
Indexes.

To allow SimpleDBM to serialize and deserialize an object from a byte-stream, 
you must:

1. Assign a unique 2-byte (short) type code to the class. Because the type 
   code gets recorded in persistent storage, 
   it must be stable, i.e., once registered, the type code association 
   for a class must remain constant for the life span of the
   the database. 

2. Ensure that the class implements a constructor that takes a 
   ByteBuffer argument. The constructor should expect the first two bytes
   to contain the typecode for the class.

3. Ensure that the class implements the Storable interface. The 
   stored length of an object of the class must allow for the 2-byte
   type code.

4. Provide an implementation of the ``org.simpledbm.rss.api.registry.ObjectFactory``
   interface, and register this implementation with the ``ObjectRegistry``.

An important consideration is to ensure that all the required classes
are available to a SimpleDBM RSS instance at startup.  

A limitation of the current design is that the type registrations are
not held in persistent storage. Since all type mappings must be available
to SimpleDBM server when it is starting up (as these may be involved
in recovery) you need to ensure that custom type mappings are registered
to the ``ObjectRegistry`` immediately after the server instance is constructed, but 
before the server is started. Typically this is handled by requiring
each module to register its own types.

Type codes between the range 0-1000 are reserved for use by
SimpleDBM.

Two Use cases
-------------

The ``ObjectRegistry`` supports two use cases.

- The first use case is where the client registers an ``ObjectFactory``
  implementation. In this case, ``ObjectRegistry`` can construct the 
  correct object from a byte stream on request. By taking care of the
  common case, the client code is simplified.
  
- The second case is when the client has a more complex requirement
  and wants to manage the instantiation of objects. In this scenario,
  the client registers a singleton class and associates this with the
  type code. The ``ObjectRegistry`` will return the registered singleton 
  object when requested. 

Registering an ObjectFactory
----------------------------

In the simple case, an ``ObjectFactory`` implementation needs to be
registered for a particular type code. 

In the example shown below, objects of the class ``MyObject`` 
are to be persisted::

 public final class MyObject implements Storable {
   final int value;
   
   /**
    * Constructs a MyObject from a byte stream.
    */
   public MyObject(ByteBuffer buf) {
     // skip the type code
     buf.getShort();
     // now get the value
     value = buf.getInt();
   }
   
   /* Storable methods not shown */
 }
 
Points worth noting are:

- The ``MyObject`` class provides a constructor that takes a ``ByteBuffer``
  as an argument. This is important as it allows the object to
  use final fields, allowing the class to be immutable.
 
- The constructor skips the first two bytes which contain the type code.

We need an ``ObjectFactory`` implementation that constructs the ``MyObject``
instances. The implementation is relatively straightforward::

 class MyObjectFactory implements ObjectFactory {
   Object getInstance(ByteBuffer buf) {
     return new MyObject(buf);
   }
 }
 
Next, we register the ``ObjectFactory`` implementation with the ``ObjectRegistry``.::

 objectRegistry.registerObjectFactory(1, new MyObjectFactory());

Given above registration, it is possible to
construct MyObject instances as follows::

 ByteBuffer buf = ...;
 MyObject o = (MyObject) objectRegistry.getInstance(buf);

This pattern is used throughout SimpleDBM RSS.

Asymmetry between retrieving and storing objects
------------------------------------------------

The reader may have noticed that the ``ObjectFactory`` says
nothing about how objects are marshalled into a byte stream.
It only deals with the unmarshalling of objects. 

The marshalling is handled by the object itself. All
objects that support marshalling are required to implement
the Storable interface.

Storable Interface and Object serialization
-------------------------------------------

Classes that need serialization support should implement
the interface ``org.simpledbm.rss.api.registry.Storable``. 
The ``Storable`` interface requires the object
to be able to predict its persistent size in bytes when the
``getStoredLength()`` method is invoked. It also requires the
implementation to be able to stream itself to a ``ByteBuffer``.

The Storable interface specification is as follows::

 /**
  * A Storable object can be written to (stored into) or 
  * read from (retrieved from) a ByteBuffer. The object 
  * must be able to predict its length in bytes;
  * this not only allows clients to allocate ByteBuffer 
  * objects of suitable size, it is also be used by a 
  * StorageContainer to ensure that objects can be
  * restored from secondary storage.
  */
 public interface Storable {
 
   /**
    * Store this object into the supplied ByteBuffer in 
    * a format that can be subsequently used to reconstruct the
    * object. ByteBuffer is assumed to be setup 
    * correctly for writing.
    * 
    * @param bb ByteBuffer that will contain a stored 
    *           representation of the object.
    */
   void store(ByteBuffer bb);

   /**
    * Predict the length of this object in bytes when 
    * stored in a ByteBuffer.
    * 
    * @return The length of this object when stored in 
    *         a ByteBuffer.
    */
   int getStoredLength();
 }

The implementation of the ``store()`` method must be the inverse
of the constructor that takes the ``ByteBuffer`` argument.

A complete example of the ``MyObject`` class will look like
this::

 public final class MyObject implements Storable {
   final int value;
   
   /**
    * Constructs a MyObject from a byte stream.
    */
   public MyObject(ByteBuffer buf) {
     // skip the type code
     buf.getShort();
     // now get the value
     value = buf.getInt();
   }

   public int getStoredLength() {
     return 6;
   }

   /**
    * Serialize to a ByteBuffer object.
    */     
   public void store(ByteBuffer bb) {
     bb.putShort((short) 1);
     bb.putInt(value);
   }
 }


Registering Singletons
----------------------

In some cases, the requirements for constructing objects are complex
enough for the client to manage it itself. In this case, the client
provides a singleton object that is registered with the ``ObjectRegistry``.
The ``ObjectRegistry.getSingleton(int typecode)`` method retrieves the
Singleton object. Typically, the singleton is a factory class that can
be used to create new instances of objects.


------------
Transactions
------------

Most SimpleDBM operations take place in the context of a Transaction.
Following are the main API calls for managing transactions.

Creating new Transactions
=========================

To start a new Transaction, invoke the ``Server.begin()`` method as
shown below. You must supply an ``IsolationMode``, try
``READ_COMMITTED`` to start with.::

 Server server = ...;

 // Start a new Transaction
 Transaction trx = server.begin(IsolationMode.READ_COMMITTED);

Isolation Modes are discussed in more detail in `Isolation Modes`_.

Working with Transactions
=========================

Transaction API
---------------

The Transaction interface provides the following methods for clients
to invoke: ::

 public interface Transaction {
 	
   /**
    * Creates a transaction savepoint.
    */
   public Savepoint createSavepoint(boolean saveCursors);
 
   /**
    * Commits the transaction. All locks held by the
    * transaction are released.
    */
   public void commit();	
 
   /**
    * Rolls back a transaction upto a savepoint. Locks acquired
    * since the Savepoint are released. PostCommitActions queued
    * after the Savepoint was created are discarded.
    */
   public void rollback(Savepoint sp);	
 
   /**
    * Aborts the transaction, undoing all changes and releasing 
    * locks.
    */
   public void abort();
 
 }

A transaction must always be either committed or aborted. Failure to
do so will lead to resource leaks, such as locks, which will not be
released.  The correct way to work with transactions is shown below: ::

 // Start a new Transaction
 Transaction trx = server.begin(IsolationMode.READ_COMMITTED);
 boolean success = false;
 try {
   // do some work and if this is completed succesfully ...
   // set success to true.
   doSomething();
   success = true;
 }
 finally {
   if (success) {
     trx.commit();
   }
   else {
     trx.abort();
   }
 }

Transaction Savepoints
----------------------

You can create transaction savepoints at any point in time.  When you
create a savepoint, you need to decide whether the scans associated
with the transaction should save their state so that in the event of
a rollback, they can be restored to the state they were in at
the time of the savepoint. This is important if you intend to use the
scans after you have performed a rollback to savepoint.

Bear in mind that in certain IsolationModes, locks are released as the
scan cursor moves, When using such an IsolationMode, rollback to a
Savepoint can fail if after the rollback, the scan cursor cannot be
positioned on a suitable location, for example, if a deadlock occurs when
it attempts to reacquire lock on the previous location. Also, in case
the location itself is no longer valid, perhaps due to a delete
operation by some other transaction, then the scan may position itself
on the next available location.

If you are preserving cursor state during savepoints, be prepared that
in certain IsolationModes, a rollback may fail due to locking, or the
scan may not be able to reposition itself on exactly the same
location.

------
Tables
------

TupleContainers
===============

SimpleDBM provides support for tables with variable length records.
The container for a table is known as ``TupleContainer``.  As far as SimpleDBM is concerned,
a row in a ``TupleContainer`` is just a blob of data; it can contain
anything you like. SimpleDBM will automatically break up a large row
into smaller chunks so the chunks can be stored in individual data
pages. This chunking is transparent from a client perspective, as the
client only ever sees full records.

Creating a TupleContainer
=========================

When you create a ``TupleContainer``, you must supply a name for the
container, a unique numeric ID which should not be in use by any other
container, and the extent size. For efficiency, SimpleDBM allocates
space in extents; an extent is simply a set of contiguous pages.::

 /**
  * Creates a new Tuple Container. 
  * 
  * @param trx Transaction to be used for creating the container
  * @param name Name of the container
  * @param containerId A numeric ID for the container - must 
  *                    be unique for each container
  * @param extentSize The number of pages that should be part 
  *                   of each extent in the container
  */
 public void createTupleContainer(Transaction trx, String name,
  int containerId, int extentSize);

Note that the ``createTupleContainer()`` method requires a Transaction.
Given below is an example of how a tuple container may be created.
In this instance, we are creating a TupleContainer named "test.db", which
will be assigned container ID 1, and will have an extent size of 20 pages.::

 Transaction trx = server.begin(IsolationMode.READ_COMMITTED);
 boolean success = false;
 try {
   server.createTupleContainer(trx, "test.db", 1, 20);
   success = true;
 }
 finally {
   if (success)
     trx.commit();			
   else 
     trx.abort();
 }

Note: 
  When you create a Container it is exclusively locked. The lock
  is released when you commit or abort the transaction. The exclusive lock
  prevents any other transaction from manipulating the container while it is
  being created.

Recommendation: 
  You should create standalone transactions for creating
  containers, and commit the transaction as soon as the container has been
  created.

Accessing a TupleContainer
==========================

To manipulate a ``TupleContainer``, you must first get access to it. This
is done by invoking the ``getTupleContainer()`` method provided by the
SimpleDBM Server object. Note that when you access a ``TupleContainer`` in
this way, a shared lock is placed on the container. This prevents
other transactions from deleting the container while you are working
with it. However, other transactions can perform row level operations
on the same container concurrently.::

 Server server ...
 
 Transaction trx = server.begin(IsolationMode.READ_COMMITTED);
 try {
   boolean success = false      
   TupleContainer container = server.getTupleContainer(trx, 1);
   // do something
   success = true;
 }
 finally {
   if (success)
     trx.commit();
   else
     trx.abort();
 }

Inserting tuples
================

SimpleDBM treats tuples (rows) as blobs of data, and does not care
about the internal structure of a tuple. The only requirement is that
a tuple must implement the ``Storable`` interface.

An insert operation is split into two steps. In the first step,
the initial chunk of the tuple is inserted and a Location assigned to
the tuple. At this point, you can do other things such as add entries 
to indexes. 

You complete the insert as a second step. At this point, if the tuple
was larger than the space allowed for in the first chunk, additional
chunks get created and allocated for the tuple.

The reason for the two step operation is to ensure that for large
tuples that span multiple pages, the insert does not proceed until it
is certain that the insert will be successful. It is assumed that once
the indexes have been successfully updated, in particular, the primary
key has been created, then the insert can proceed.

In the example below, we insert a tuple of type ``ByteString``, which is
a ``Storable`` wrapper for ``String`` objects.::

 Server server ...
 
 Transaction trx = server.begin(IsolationMode.READ_COMMITTED);
 try {
   boolean success = false      
   TupleContainer container = server.getTupleContainer(trx, 1);
   TupleInserter inserter = 
     container.insert(trx, new ByteString("Hello World!"));
   Location location = inserter.getLocation();
  
   // Create index entires here
 
   inserter.completeInsert();
   success = true;
 }
 finally {
   if (success)
     trx.commit();
   else
     trx.abort();
 }

Reading tuples
==============

In order to read tuples, you must open a scan. A scan is a mechanism
for accessing tuples one by one. You can open Index Scans (described
in next chapter) or Tuple Scans. A Tuple Scan directly scans a
TupleContainer.  Compared to index scans, tuple scans are unordered,
and do not support Serializable or Repeatable Read lock modes. Another
limitation at present is that tuple scans do not save their state
during savepoints, and therefore cannot restore their state in the event of
a rollback to a savepoint.::

 Server server ...
 
 Transaction trx = server.begin(IsolationMode.READ_COMMITTED);
 try {
   boolean success = false      
   TupleContainer container = server.getTupleContainer(trx, 1);
   TupleScan scan = container.openScan(trx, false);
   while (scan.fetchNext()) {
     byte[] data = scan.getCurrentTuple();
     // Do somthing with the tuple data
   }
   success = true;
 }
 finally {
   if (success)
     trx.commit();
   else
     trx.abort();
 }


Updating tuples
===============

In order to update a tuple, you must first obtain its Location using a
scan. typically, if you intend to update the tuple, you should open the
scan in UPDATE mode. This is done by supplying a boolean true as the
second argument to ``openScan()`` method.

Note that in the current implementation of SimpleDBM, the space
allocated to a tuple is never reduced, even if the tuple grows smaller
due to updates.::

 Server server ...
 
 Transaction trx = server.begin(IsolationMode.READ_COMMITTED);
 try {
   boolean success = false      
   TupleContainer container = server.getTupleContainer(trx, 1);
   TupleScan scan = container.openScan(trx, true);
   while (scan.fetchNext()) {
     Location location = scan.getCurrentLocation();
     byte[] data = scan.getCurrentTuple();
     // Do somthing with the tuple data
     // Assume updatedTuple contains update tuple data.
     Storable updatedTuple = ... ;
     // Update the tuple
     container.update(trx, location, updatedTuple);
   }
   success = true;
 }
 finally {
   if (success)
     trx.commit();
   else
     trx.abort();
  }
 
Deleting tuples
===============
 
Tuple deletes are done in a similar way as tuple updates.
Start a scan in UPDATE mode, if you intend to delete tuples
during the scan. Here is an example: ::
 
 Server server ...
 
 Transaction trx = server.begin(IsolationMode.READ_COMMITTED);
 try {
   boolean success = false      
   TupleContainer container = server.getTupleContainer(trx, 1);
   TupleScan scan = container.openScan(trx, true);
   while (scan.fetchNext()) {
     Location location = scan.getCurrentLocation();
     container.delete(trx, location);
   }
   success = true;
 }
 finally {
   if (success)
     trx.commit();
   else
     trx.abort();
 }

-------
Indexes
-------

Index Keys
==========

Index Keys are the searchable attributes stored in the Index. This module
specifies Index Keys in a fairly general way: ::

 public interface IndexKey 
   extends Storable, Comparable<IndexKey> {
 	
   /**
    * Used mainly for building test cases; this method should
    * parse the input string and initialize itself. The contents 
    * of the string is expected to match the toString() output.
    */
   void parseString(String string);
 }

The requirements for an IndexKey are fairly simple. The key must be
``Storable`` and ``Comparable``. Note that this interface does
not say anything about the internal structure of the key; in
particular it does not specify whether the key contains multiple
attributes. This is deliberate, as it makes the Index Manager module
more generic.

Depending upon the implementation, there may be restricions on the
size of the key. For instance, in the SimpleDBM BTree implementation,
the key should not exceed 1/8th of the page size, ie, 1KB in a 8KB
page.

Index Key Factories
===================

An Index Key Factory is used to create new keys. In addition to normal
keys, an Index Key Factory must be able to create two special type of
keys:

MinKey
  This is a special key that represents negative
  infinity. All valid keys must be greater than this key.

MaxKey
  This is a special key that represents positive
  infinity. All valid keys must be less than this key.

The special keys are used by the Index Manager internally. You can
also specify the ``MinKey`` when opening a scan, to effectively
start the scan from the first available key.::

 public interface IndexKeyFactory {
 
   /**
    * Generates a new (empty) key for the specified
    * Container. The Container ID is meant to be used as key
    * for locating information specific to a container; for 
    * instance, the attributes of an Index.
    * 
    * @param containerId ID of the container for which a key 
    *                    is required
    */
   IndexKey newIndexKey(int containerId);
 
   /**
    * Generates a key that represents Infinity - it must be 
    * greater than all possible keys in the domain for the key. 
    * The Container ID is meant to be used as key
    * for locating information specific to a container; for 
    * instance, the attributes of an Index.
    * 
    * @param containerId ID of the container for which a key 
    *                    is required
    */
   IndexKey maxIndexKey(int containerId);
 	
   /**
    * Generates a key that represents negative Infinity - it 
    * must be smaller than all possible keys in the domain for 
    * the key. The Container ID is meant to be used as key
    * for locating information specific to a container; for 
    * instance, the attributes of an Index.
    *
    * The key returned by this method can be used as an
    * argument to index scans. The result will be a scan of 
    * the index starting from the first key in the index.
    * 
    * @param containerId ID of the container for which a key 
    * is required
    */
   IndexKey minIndexKey(int containerId);
 }

An implementation is free to use any method it likes for identifying
keys that represent ``MinKey`` and ``MaxKey``, as long as it ensures
that these keys will obey the contract defined above.

The methods of ``IndexKeyFactory`` take the container ID as a
parameter. In SimpleDBM, each index is stored in a separate container,
hence container ID is used as a mechanism for identifying the index.
The ``IndexKeyFactory`` implementation is expected to use the
container ID to determine the type of index key to create. For
example, it may consult a data dictionary to determine the type of key
attributes required by the index key.

Example
-------

Given below is an example of an ``IndexKey`` implementation. This
implementation uses a special byte field to maintain the status
information.::

 public class IntegerKey implements IndexKey {
 
   private static final byte NULL_FIELD = 1;
   private static final byte MINUS_INFINITY_FIELD = 2;
   private static final byte VALUE_FIELD = 4;
   private static final byte PLUS_INFINITY_FIELD = 8;
 
   private byte statusByte = NULL_FIELD;
   private int value;
 
   public IntegerKey() {
     statusByte = NULL_FIELD;
   }
 
   public IntegerKey(int value) {
     statusByte = VALUE_FIELD;
     this.value = value;
   }
 
   protected IntegerKey(byte statusByte, int value) {
     this.statusByte = statusByte;
     this.value = value;
   }
 
   public int getValue() {
     if (statusByte != VALUE_FIELD) {
       throw new IllegalStateException("Value has not been set");
     }
     return value;
   }
 
   public void setValue(int i) {
     value = i;
     statusByte = VALUE_FIELD;
   }
 
   public void retrieve(ByteBuffer bb) {
     statusByte = bb.get();
     value = bb.getInt();
   }
 
   public void store(ByteBuffer bb) {
     bb.put(statusByte);
     bb.putInt(value);
   }
 
   public int getStoredLength() {
     return 5;
   }
 
   public int compareTo(IndexKey key) {
     if (key == null) {
       throw new IllegalArgumentException("Supplied key is null");
     }
     if (key == this) {
       return 0;
     }
     if (key.getClass() != getClass()) {
       throw new IllegalArgumentException(
           "Supplied key is not of the correct type");
     }
     IntegerKey other = (IntegerKey) key;
     int result = statusByte - other.statusByte;
     if (result == 0 && statusByte == VALUE_FIELD) {
       result = value - other.value;
     }
     return result;
   }
 
   public final boolean isNull() {
     return statusByte == NULL_FIELD;
   }
 
   public final boolean isMinKey() {
     return statusByte == MINUS_INFINITY_FIELD;
   }
 
   public final boolean isMaxKey() {
     return statusByte == PLUS_INFINITY_FIELD;
   }
 
   public final boolean isValue() {
     return statusByte == VALUE_FIELD;
   }
 
   public boolean equals(Object o) {
     if (o == null) {
       throw new IllegalArgumentException("Supplied key is null");
     }
     if (o == this) {
       return true;
     }
     if (o == null || !(o instanceof IntegerKey)) {
       return false;
     }
     return compareTo((IntegerKey) o) == 0;
   }
 
   public void parseString(String s) {
     if ("<NULL>".equals(s)) {
       statusByte = NULL_FIELD;
     } else if ("<MINKEY>".equals(s)) {
       statusByte = MINUS_INFINITY_FIELD;
     } else if ("<MAXKEY>".equals(s)) {
       statusByte = PLUS_INFINITY_FIELD;
     } else {
       value = Integer.parseInt(s);
       statusByte = VALUE_FIELD;
     }
   }
 
   public String toString() {
     if (isNull()) {
       return "<NULL>";
     } else if (isMinKey()) {
       return "<MINKEY>";
     } else if (isMaxKey()) {
       return "<MAXKEY>";
     } else {
       return Integer.toString(value);
     }
   }
 
   public static IntegerKey createNullKey() {
     return new IntegerKey(NULL_FIELD, 0);
   }
 
   public static IntegerKey createMinKey() {
     return new IntegerKey(MINUS_INFINITY_FIELD, 0);
   }
 
   public static IntegerKey createMaxKey() {
     return new IntegerKey(PLUS_INFINITY_FIELD, 0);
   }
 }

Shown below is the corresponding ``IndexKeyFactory`` implementation.::

 public class IntegerKeyFactory implements IndexKeyFactory {
 
   public IndexKey maxIndexKey(int containerId) {
     return IntegerKey.createMaxKey();
   }
 
   public IndexKey minIndexKey(int containerId) {
     return IntegerKey.createMinKey();
   }
 
   public IndexKey newIndexKey(int containerId) {
     return IntegerKey.createNullKey();
   }
 }

The example shown above is a simple key. It is possible to create 
multi-attribute keys as well. For an example of how this can be done,
please see the sample ``typesystem`` package supplied with SimpleDBM,
and the sample project ``tupledemo``.

Locations
=========

Indexes contain pointers to tuple data. When you insert a tuple in a
tuple container, a location is assigned to the tuple. This location
can be stored in an index to point to the tuple.

Creating a new index
====================

Following code snippet shows the steps required to create a new
index.::

 int INDEX_KEY_FACTORY_TYPE = 25000;
 
 Server server = ...;
 IndexKeyFactory indexKeyFactory = ...;
 
 // Register key factory
 server.registerSingleton(INDEX_KEY_FACTORY_TYPE, 
   indexKeyFactory);
 
 Transaction trx = server.begin(IsolationMode.READ_COMMITTED);
 boolean success = false;
 try {
   int containerId = 1;
   int extentSize = 8;
   boolean isUnique = true;
   server.createIndex(trx, "testbtree.dat", 
     containerId, extentSize, INDEX_KEY_FACTORY_TYPE, 
     isUnique);
   success = true;
 } finally {
   if (success)
     trx.commit();
   else
     trx.abort();
 }

Obtaining index instance
========================

In order to manipulate an index, you must first obtain an instance of
the index. This is shown below. ::

 Transaction trx = ...;
 int containerId = 1;
 IndexContainer index = server.getIndex(trx, containerId);

This operation causes a SHARED mode lock to be placed on the container
ID. This lock is to prevent other transactions from dropping the
container. Concurrent insert, delete and scan operations are
permitted, however.

Working with keys
=================

Adding a key
------------

The API for inserting new keys is shown below. ::

 public interface IndexContainer {
   /**
    * Inserts a new key and location. If the Index is unique, 
    * only one instance of key is allowed. In non-unique indexes, 
    * multiple instances of the same key may exist, but only
    * one instance of the combination of key/location
    * is allowed.
    *
    * The caller must obtain a Shared lock on the Index 
    * Container prior to this call.
    * 
    * The caller must acquire an Exclusive lock on Location 
    * before this call.
    * 
    * @param trx Transaction managing the insert
    * @param key Key to be inserted
    * @param location Location associated with the key
    */
   public void insert(Transaction trx, IndexKey key, 
                      Location location);
 }

To add a key, you need the ``IndexKey`` instance and the
``Location``. The most common use case is to insert the keys
after inserting a tuple in a tuple container. An example of this is
shown below: ::

 Transaction trx = server.begin(IsolationMode.READ_COMMITTED);
 boolean success = false;
 try {
   TupleContainer table = server.getTupleContainer(
     trx, TABLE_CONTNO);
   IndexContainer primaryIndex = 
     server.getIndex(trx, PKEY_CONTNO);
   IndexContainer secondaryIndex = 
     server.getIndex(trx, SKEY1_CONTNO);
 
   // First lets create a new row and lock the location
   TupleInserter inserter = table.insert(trx, tableRow);
 
   // Insert the primary key - may fail with unique constraint
   // violation
   primaryIndex.insert(trx, primaryKeyRow, 
     inserter.getLocation());
   
   // Insert seconary key
   secondaryIndex.insert(trx, secondaryKeyRow, 
     inserter.getLocation());
 
   // Complete the insert - may be a no-op.
   inserter.completeInsert();
   success = true;
 } finally {
   if (success) {
     trx.commit();
   } else {
     trx.abort();
   }
 }

Above example is taken from the sample ``tupledemo``. 

Deleting a key
--------------

Deleting a key is very similar to adding a key. First lets look
at the API. ::

 public interface IndexContainer {
   /**
    * Deletes specified key and location. 
    * 
    * The caller must obtain a Shared lock on the Index 
    * Container prior to this call.
    *
    * The caller must acquire an Exclusive lock on Location 
    * before this call.
    * 
    * @param trx Transaction managing the delete
    * @param key Key to be deleted
    * @param location Location associated with the key
    */
   public void delete(Transaction trx, IndexKey key, 
                      Location location);
 }

Again we take an example from the tupledemo sample (note that
the code has been simplified a bit). ::

 // Start a new transaction
 Transaction trx = server.begin(IsolationMode.READ_COMMITTED);
 boolean success = false;
 try {
   TupleContainer table = 
     server.getTupleContainer(trx, TABLE_CONTNO);
   IndexContainer primaryIndex = 
     server.getIndex(trx, PKEY_CONTNO);
 
   // Start a scan, with the primary key as argument
   IndexScan indexScan = primaryIndex.openScan(trx, primaryKeyRow,
     null, true);
   if (indexScan.fetchNext()) {
     // Scan always return item >= search key, so let's
     // check if we had an exact match
     boolean matched = indexScan.getCurrentKey().equals(
       primaryKeyRow);
     try {
       if (matched) {
         Location location = indexScan.getCurrentLocation();
         // Delete tuple data
         table.delete(trx, location);
         // Delete primary key
         primaryIndex.delete(trx, primaryKeyRow, location);
       }
     } finally {
       indexScan.fetchCompleted(matched);
     }
   }
   success = true;
 } finally {
   if (success) {
     trx.commit();
   } else {
     trx.abort();
   }
 }

Prior to deleting the key, the associated location must be
exclusively locked for commit duration. Fortunately, when you delete
a tuple, it is locked exclusively, hence in the example shown above,
there is no need for the Location to be locked again.

Above example demonstrates also a very common use case, ie, scanning
an index in UPDATE mode and then carrying out modifications. 

Scanning keys
=============

Isolation Modes
---------------

Before describing how to scan keys within an Index, it is necessary to
describe the various lock isolation modes supported by SimpleDBM.

Common Behaviour
----------------

Following behaviour is common across all lock isolation modes.

1. All locking is on Tuple Locations (rowids) only.
2. When a tuple is inserted or deleted, its Location is first
   locked in EXCLUSIVE mode, the tuple is inserted or deleted from data
   page, and only after that, indexes are modified.
3. Updates to indexed columns are treated as key deletes followed
   by key inserts. The updated row is locked in EXCLUSIVE mode before
   indexes are modified.
4. When fetching, the index is looked up first, which causes a
   SHARED or UPDATE mode lock to be placed on the row, before the data
   pages are accessed.

Read Committed/Cursor Stability
-------------------------------

During scans, the tuple location is locked in SHARED or UPDATE mode
while the cursor is positioned on the key. The lock on current
location is released before the cursor moves to the next key.

Repeatable Read (RR)
--------------------

SHARED mode locks obtained on tuple locations during scans are retained until
the transaction completes. UPDATE mode locks are downgraded to SHARED mode when
the cursor moves.

Serializable
------------

Same as Repeatable Read, with additional locking (next key) during
scans to prevent phantom reads.

Scanning API
------------

Opening an IndexScan requires you to specify a start condition.
If you want to start from the beginning, then you may specify null values
as the start key/location. 

In SimpleDBM, scans do not have a stop key. Instead, a scan starts fetching
data from the first key/location that is greater or equal to the supplied
start key/location. You must determine whether the fetched key satisfies
the search criteria or not. If the fetched key no longer meets the search
criteria, you should call ``fetchCompleted()`` with a false value, indicating that
there is no need to fetch any more keys. This then causes the scan to
reach logical EOF.

The API for index scans is shown below: ::

 public interface IndexContainer {
 	
   /**
    * Opens a new index scan. The Scan will fetch keys >= the 
    * specified key and location. Before returning fetched keys, 
    * the associated Location objects will be locked. The lock 
    * mode depends upon the forUpdate flag. The IsolationMode
    * of the transaction determines when lock are released. 
    * 
    * Caller must obtain a Shared lock on the Index Container 
    * prior to calling this method.
    * 
    * @param trx Transaction that will manage locks obtained 
    *            by the scan
    * @param key The starting key to be searched for
    * @param location The starting location to be searched for.
    * @param forUpdate If this set, UPDATED mode locks will 
    *                  be acquired, else SHARED mode locks will
    *                  be acquired.
    */
   public IndexScan openScan(Transaction trx, IndexKey key, 
     Location location, boolean forUpdate);	
 	
 }
 
 
 public interface IndexScan {
 	
   /**
    * Fetches the next available key from the Index. 
    * Handles the situation where current key has been deleted.
    * Note that prior to returning the key the Location 
    * object associated with the key is locked.
    * After fetching an index row, typically, data must 
    * be fetched from associated tuple container. Locks 
    * obtained by the fetch protect such access. After 
    * tuple has been fetched, caller must invoke 
    * fetchCompleted() to ensure that locks are released 
    * in certain lock isolation modes. Failure to do so will 
    * cause extra locking.
    */
   public boolean fetchNext();
 	
   /**
    * In certain isolation modes, releases locks acquired 
    * by fetchNext(). Must be invoked after the data from 
    * associated tuple container has been fetched.
    * If the argument matched is set to false, the scan 
    * is assumed to have reached eof of file. The next
    * call to fetchNext() will return false.
    * 
    * @param matched If set to true indicates that the 
    *                key satisfies search query
    */
   public void fetchCompleted(boolean matched);
 	
   /**
    * Returns the IndexKey on which the scan is currently 
    * positioned.
    */
   public IndexKey getCurrentKey();
 	
   /**
    * Returns the Location associated with the current 
    * IndexKey.
    */
   public Location getCurrentLocation();
 	
   /**
    * After the scan is completed, the close method 
    * should be called to release all resources acquired 
    * by the scan.
    */
   public void close();
 	
   /**
    * Returns the End of File status of the scan. Once 
    * the scan has gone past the last available key in 
    * the Index, this will return true.  
    */
   public boolean isEof();
 }


Following code snippet, taken from the btreedemo sample,
shows how to implement index scans.::

 Transaction trx = ...;
 IndexContainer indexContainer = ...;
 IndexScan scan = indexContainer.openScan(trx, null, 
   null, false);
 try {
   while (scan.fetchNext()) {
     System.err.println("SCAN NEXT=" + scan.getCurrentKey() + 
       "," + scan.getCurrentLocation());
     scan.fetchCompleted(true);
   }
 } finally {
   if (scan != null) {
     scan.close();
   }
 }

Another example of an index scan can be found in section `Deleting a key`_.

