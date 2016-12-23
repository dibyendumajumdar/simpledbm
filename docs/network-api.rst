
.. -*- coding: utf-8 -*-

=====================
SimpleDBM Network API
=====================

:Author: Dibyendu Majumdar
:Contact: d dot majumdar at gmail dot com
:Version: 1.0.23
:Date: 17 March 2010 
:Copyright: Copyright by Dibyendu Majumdar, 2010-2016

.. contents::

------------
Introduction
------------

This document describes the SimpleDBM Network API.

Intended Audience
=================

This documented is targetted at users of `SimpleDBM <https://github.com/dibyendumajumdar/simpledbm>`_.

Pre-requisite Reading
=====================

Before reading this document, the reader is advised to go through 
the `SimpleDBM Overview <http://simpledbm.readthedocs.io/en/latest/overview.html>`_ document.

---------------
Getting Started
---------------

SimpleDBM binaries
==================
SimpleDBM makes use of Java 5.0 features, hence you will need to use JDK1.6
or above if you want to work with SimpleDBM.

The following maven dependencies give you access to the server jars.

::

  <dependency>
    <groupId>org.simpledbm</groupId>
    <artifactId>simpledbm-network-server</artifactId>
    <version>1.0.23</version>
  </dependency>

The following maven dependencies are needed for the client application.

::

  <dependency>
    <groupId>org.simpledbm</groupId>
    <artifactId>simpledbm-network-server</artifactId>
    <version>1.0.23</version>
  </dependency>
  <dependency>
    <groupId>org.simpledbm</groupId>
    <artifactId>simpledbm-network-framework</artifactId>
    <version>1.0.23</version>
  </dependency>
  <dependency>
    <groupId>org.simpledbm</groupId>
    <artifactId>simpledbm-network-common</artifactId>
    <version>1.0.23</version>
  </dependency>
  <dependency>
    <groupId>org.simpledbm</groupId>
    <artifactId>simpledbm-typesystem</artifactId>
    <version>1.0.23</version>
  </dependency>


Summary of Steps Required
=========================

1. Write your client application, using the SimpleDBM Network Client API.
2. Create a new SimpleDBM database.
3. Start the SimpleDBM Network Server.
4. Run your application.

Writing your Application
========================
At present only Java language bindings are available, therefore you must write your application
in Java. All you need is the SimpleDBM Network Client jar, which includes required 
SimpleDBM modules for interacting with the server.

This document will in future contain a tutorial on how to use the Client API. For now,
the Java Interface for the API is described in the Appendix. An example client interaction
is given below::

  Properties properties = parseProperties("test.properties");

An example test.properties file is given in the next section.
Start a session::

  SessionManager sessionManager = sm = SessionManager.getSessionManager(properties, 
    "localhost", 8000,
    (int) TimeUnit.MILLISECONDS.convert(5 * 60, TimeUnit.SECONDS));

The last parameter is the socket timeout in milliseconds. The socket will timeout
when reading/writing when the specified timeout period is exceeded and there is no
response from the server. 

Each SessionManager instance maintains a single network connection to
SimpleDBM Server. In order to interact with the server, you need to open sessions.
Each session is simply a transaction context, allowing you to have one active
transaction per session.

Here we open a session, obtain the type factoy and create a table definition::

  // Get the type factory
  TypeFactory ff = sessionManager.getTypeFactory();
  // Open a session
  Session session = sessionManager.openSession();
  try {
   // create a table definition
   TypeDescriptor employee_rowtype[] = { ff.getIntegerType(), /* pk */
   ff.getVarcharType(20), /* name */
   ff.getVarcharType(20), /* surname */
   ff.getVarcharType(20), /* city */
   ff.getVarcharType(45), /* email address */
   ff.getDateTimeType(), /* date of birth */
   ff.getNumberType(2) /* salary */
   };
   // the table will be assigned container ID 1.
   // Containers identify the files that will store the
   // data and therefore must be unique.
   TableDefinition tableDefinition = sessionManager
     .newTableDefinition("employee", 1, employee_rowtype);
   // define a few indexes
   tableDefinition.addIndex(2, "employee1.idx", new int[] { 0 }, true,
     true);
   tableDefinition.addIndex(3, "employee2.idx", new int[] { 2, 1 },
     false, false);
   tableDefinition.addIndex(4, "employee3.idx", new int[] { 5 },
     false, false);
   tableDefinition.addIndex(5, "employee4.idx", new int[] { 6 },
     false, false);

Now we can create the table in the database. This is
done in an internal transaction that you cannot control.::

   session.createTable(tableDefinition);

Now that the table has been created, we can initiate a transaction
and insert a row::

   // Start transaction
   session.startTransaction(IsolationMode.READ_COMMITTED);
   boolean success = false;
   try {
    /*
     * Each table is identified the container ID that was
     * assigned when defining the table. So in this
     * case the container ID is 1.
     */
    Table table = session.getTable(1);
    // Get a blank row
    Row tableRow = table.getRow();
    // Initialize the row
    tableRow.setInt(0, 1);
    tableRow.setString(1, "Joe");
    tableRow.setString(2, "Blogg");
    tableRow.setDate(5, getDOB(1930, 12, 31));
    tableRow.setString(6, "500.00");
    // Insert the row
    table.addRow(tableRow);

In the same transaction, let us scan through the rows in the table::

    // The first parameter of the scan is the index
    // The second parameter is the search row. In this case
    // we want to scan all rows. The last argument is whether
    // we intend to update rows.
    TableScan scan = table.openScan(0, null, false);
    try {
     // Get the next row
     Row row = scan.fetchNext();
     while (row != null) {
      System.out.println("Fetched row " + row);
      // Lets change one of the fields
      row.setString(6, "501.00");
      // Update the current row
      scan.updateCurrentRow(row);
      // Get the next row
      row = scan.fetchNext();
     }
    } finally {
     scan.close();
    }
    success = true;

Finally we commit the transaction::

   } finally {
    if (success) {
     session.commit();
    } else {
     session.rollback();
    }
   }

Now lets delete the newly added row.
First start a new transaction::

   session.startTransaction(IsolationMode.READ_COMMITTED);
   success = false;
   try {
    Table table = session.getTable(1);

Scan the table and delete all rows::

    TableScan scan = table.openScan(0, null, false);
    try {
     Row row = scan.fetchNext();
     while (row != null) {
      System.out.println("Deleting row " + row);
      scan.deleteRow();
      row = scan.fetchNext();
     }
    } finally {
     scan.close();
    }
    success = true;

Commit the transaction::

   } finally {
    if (success) {
     session.commit();
    } else {
     session.rollback();
    }
   }
  } catch (Exception e) {
   e.printStackTrace();

Finally, close the session::

  } finally {
   session.close();
  }

Note that you can only have one transaction active in the context of
a session. If you need to have more than one transaction active, each 
should be given its own session context.

When you close a session, any pending transaction will be aborted
unless you have already committed the transaction. It is always 
preferable to explicitly commit or abort transactions.

The server also has a session timeout feature which enables it to
clean up sessions that are idle for a while. It is not a good idea to
leave a session idle for long; you can close the session once you are done
and open a new one when necessary.

Creating a SimpleDBM database
=============================

The database configuration is defined in a properties file. Example of the
properties file::

  logging.properties.file = classpath:simpledbm.logging.properties
  logging.properties.type = log4j
  network.server.host = localhost
  network.server.port = 8000
  network.server.sessionTimeout = 300000
  network.server.sessionMonitorInterval = 120
  network.server.selectTimeout = 10000
  log.ctl.1 = ctl.a
  log.ctl.2 = ctl.b
  log.groups.1.path = .
  log.archive.path = .
  log.group.files = 3
  log.file.size = 5242880
  log.buffer.size = 5242880
  log.buffer.limit = 4
  log.flush.interval = 30
  log.disableFlushRequests = true
  storage.basePath = testdata/DatabaseTests
  storage.createMode = rw
  storage.openMode = rw
  storage.flushMode = noforce
  bufferpool.numbuffers = 1500
  bufferpool.writerSleepInterval = 60000
  transaction.ckpt.interval = 60000
  lock.deadlock.detection.interval = 3

Notice that most of these properties are the standard options supported by SimpleDBM.
An example of the logging properties file can be found in the SimpleDBM
distribution.

The additional properties that are specific to the network server are
described below:

network.server.host
  DNS name or ip address of the server

network.server.port
  Port on which the server is listening for connections

network.server.sessionTimeout
  The session timeout in milliseconds. If a session is idle for longer than
  this duration, it will be closed. Any pending transaction will be aborted.

network.server.sessionMonitorInterval
  The frequency (in seconds) at which the server checks for idle sessions.

network.server.selectTimeout 
  The network server uses the select() facility to poll for network
  requests. Rather than blocking indefinitely, it uses the specified timeout
  value. This allows the server to wake up every so often; the default value
  of 10000 milliseconds is fine and need not be changed.

To create your new database, invoke SimpleDBM Network Server as follows:

::

  java -jar simpledbm-network-server-1.0.23.jar create <properties file>

This will create an empty database in the location specified by the property
`storage.basePath`.

Note that you can obtain the jar above from Maven Central - the link is 
`SimpleDBM NetWork Server 1.0.23 <http://search.maven.org/remotecontent?filepath=org/simpledbm/simpledbm-network-server/1.0.23/simpledbm-network-server-1.0.23.jar>`_.

Starting a database
===================

Once a database has been created, it can be started using the following
command (the command is wrapped into two lines but is a single command):

::

  java -Xms128m -Xmx1024m -jar simpledbm-network-server-1.0.23.jar 
     open <properties file>

To stop the database server, simply press Control-C. It may take a few 
seconds for the server to acknowledge the shutdown request.

Problems starting a database
============================

SimpleDBM uses a lock file to determine whether an instance is already
running. At startup, it creates the file at the location ``_internal\lock`` relative
to the path where the database is created. If this file already exists, then
SimpleDBM will report a failure such as::

  SIMPLEDBM-EV0005: Error starting SimpleDBM RSS Server, another
  instance may be running - error was: SIMPLEDBM-ES0017: Unable to create
  StorageContainer .._internal\lock because an object of the name already exists

This message indicates either that some other instance is running, or that
an earlier instance of SimpleDBM terminated without properly sutting down.
If the latter is the case, then the ``_internal/lock`` file may be deleted enabling
SimpleDBM to start.
 
Managing log messages
=====================

SimpleDBM has support for JDK 1.4 style logging.

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
 org.simpledbm.network.level = INFO
 org.simpledbm.network.server.level = INFO

By default, SimpleDBM looks for a logging properties file named
"simpledbm.logging.properties".

---------------
The Network API
---------------

SessionManager
==============

::

  /**
   * The SessionManager manages the connection to the SimpleDBM Network Server,
   * and initiates sessions used by the clients. Each SessionManager maintains
   * a single connection to the server. Requests sent over a single connection
   * are serialized.
   */
  public abstract class SessionManager {
    
    /**
     * Obtains an instance of the SessionManager for the specified connection
     * parameters. The client should allow for the fact that the returned
     * instance may be a shared one.
     * 
     * @param properties A set of properties - at present only logging parameters
     *                   are used
     * @param host       The DNS name or IP address of the server
     * @param port       The port the server is listening on
     * @param timeout    The socket timeout in milliseconds. This is the
     *                   timeout for read/write operations.
     * @return A Session Manager object
     */
    public static SessionManager getSessionManager(
                  Properties properties, 
                  String host, 
                  int port, 
                  int timeout);
    
    /**
     * Gets the TypeFactory associated with the database.
     */
    public abstract TypeFactory getTypeFactory();
    
    /**
     * Gets the RowFactory for the database.
     */
    public abstract RowFactory getRowFactory();
    
    /**
     * Creates a new TableDefinition.
     * 
     * @param name Name of the table's container
     * @param containerId ID of the container; must be unique
     * @param rowType The row definition as an arry of TypeDescriptors
     * @return A TableDefinition object
     */
    public abstract TableDefinition newTableDefinition(
                    String name, int containerId,
                    TypeDescriptor[] rowType);
        
    /**
     * Starts a new session.
     */
    public abstract Session openSession();
      
    /**
     * Gets the underlying connection object associated with 
     * this SessionManager.
     * 
     * The connection object must be handled with care, as 
     * its correct operation is vital to the client server 
     * communication.
     */
    public abstract Connection getConnection();    
    
    /**
     * Closes the SessionManager and its connection with the database,
     * releasing any acquired resources.
     */
    public abstract void close();
  }

Session
=======

::

  /**
   * A Session encapsulates an interactive session with the server. Each session
   * can only have one active transaction at any point in time. Clients can open
   * multiple simultaneous sessions.
   *
   * All sessions created by a SessionManager share a single network connection
   * to the server.
   */
  public interface Session {
    
    /**
     * Closes the session. If there is any outstanding transaction, it will
     * be aborted. Sessions should be closed by client applications when no 
     * longer required, as this will free up resources on the server.
     */
    public void close();
    
    /**
     * Starts a new transaction. In the context of a session, only one
     * transaction can be active at a point in time, hence if this method will
     * fail if there is already an active transaction.
     * 
     * @param isolationMode Lock isolation mode for the transaction
     */
    public void startTransaction(IsolationMode isolationMode);
    
    /**
     * Commits the current transaction; an exception will be thrown if 
     * there is no active transaction.
     */
    public void commit();
    
    /**
     * Aborts the current transaction; an exception will be thrown if 
     * there is no active transaction
     */
    public void rollback();
    
    /**
     * Creates a table as specified. The table will be created using its own
     * transaction independent of the transaction managed by the session.
     * 
     * @param tableDefinition The TableDefinition
     */
    public void createTable(TableDefinition tableDefinition);
    
    /**
     * Obtains a reference to the table. The Table container will be
     * locked in SHARED mode.
     * 
     * @param containerId The ID of the table's container
     * @return A Table object
     */
    public Table getTable(int containerId);
    
    /**
     * Gets the SessionManager that is managing this session.
     */
    public SessionManager getSessionManager();
    
    /**
     * Gets the unique id associated with this session.
     */
    public int getSessionId();
  }

 
Table
=====

::

  /**
   * A Table represents a collection of related containers, one of which is
   * a Data Container, and the others, Index Containers. The Data Container 
   * hold rows of table data, and the Index Containers provide access paths to
   * the table rows. At least one index must be created because the database
   * uses the index to manage the primary key and lock isolation modes.
   * 
   * @author Dibyendu Majumdar
   */
  public interface Table {
    
   /**
    * Starts a new Table Scan which allows the client to iterate through
    * the table's rows.
    * 
    * @param indexno The index to be used; first index is 0, second 1, etc.
    * @param startRow The search key - a suitable initialized table row.
    *                 Only columns used in the index are relevant.
    *                 This parameter can be set to null if the scan 
    *                 should start from the first available row
    * @param forUpdate A boolean flag to indicate whether the client
    *                  intends to update rows, in which case this parameter
    *                  should be set to true. If set, rows will be 
    *                  locked in UPDATE mode to allow subsequent updates.
    * @return A TableScan object
    */
    public TableScan openScan(int indexno, Row startRow,
         boolean forUpdate);
    	
    /**
     * Obtains an empty row, in which all columns are set to NULL.
     * @return
     */
    public Row getRow();
    	
    /**
     * Adds the given row to the table. The add operation may fail
     * if another row with the same primary key already exists.
     * @param row Row to be added
     */
    public void addRow(Row row);
  }

  
TableScan
=========

::

  /**
   * A TableScan is used to traverse the rows in a table, ordered
   * by an Index. The initial position of the scan is determined by
   * the keys supplied when the scan is opened. The table scan 
   * respects the lock isolation mode of the transaction.
   * 
   * As rows are fetched, the scan maintains its position. The current
   * row may be updated or deleted. 
   */
  public interface TableScan {
    
    /**
     * Fetches the next row. If EOF is reached, null will 
     * be returned.
     */
    public Row fetchNext();
    
    /**
     * Updates the current row.
     * 
     * @param tableRow New value for the row
     */
    public void updateCurrentRow(Row tableRow);
    
    /**
     * Deletes the current row.
     */
    public void deleteRow();  
    
    /**
     * Closes the scan, releasing any locks that are not required.
     */
    public void close();
    
    /**
     * Obtains the session that is associated with this scan.
     */
    Session getSession();
  }
