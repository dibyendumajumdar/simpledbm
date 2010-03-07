
.. -*- coding: utf-8 -*-

=====================
SimpleDBM Network API
=====================

:Author: Dibyendu Majumdar
:Contact: d dot majumdar at gmail dot com
:Version: 1.0.13
:Date: 06 March 2010
:Copyright: Copyright by Dibyendu Majumdar, 2010

.. contents::

------------
Introduction
------------

This document describes the SimpleDBM Network API.

Intended Audience
=================

This documented is targetted at users of `SimpleDBM <http://www.simpledbm.org>`_.

Pre-requisite Reading
=====================

Before reading this document, the reader is advised to go through 
the `SimpleDBM Overview <http://simpledbm.googlecode.com/hg/simpledbm-docs/docs/html/overview.html>`_ document.

---------------
Getting Started
---------------

SimpleDBM binaries
==================
SimpleDBM makes use of Java 5.0 features, hence you will need to use JDK1.5
or above if you want to work with SimpleDBM.

You can download the SimpleDBM binaries from the SimpleDBM GoogleCode
project download area. The following jar files are required:

* `simpledbm-common-1.0.x.jar <http://simpledbm.googlecode.com/files/>`_ - this provides common utilities.
* `simpledbm-rss-1.0.x.jar <http://simpledbm.googlecode.com/files/>`_ - this is the core database engine.

You should make sure that required jars are in your class path.

Summary of Steps Required
=========================

1. Write your client application, using the SimpleDBM Network Client API.
2. Create a new SimpleDBM database.
3. Start the SimpleDBM Network Server.
4. Run your application.

Writing your Application
========================
At present only Java language bindings are available, therefore you must write your application
in Java. All you need is the SimpleDBM Network Client jar, which includes all other required 
SimpleDBM modules.

This document will in future contain a tutorial on how to use the Client API. For now,
the Java Interface for the API is available in the Appendix. An example client interaction
is given below::

  Properties properties = parseProperties("test.properties");

Start a session::

  SessionManager sessionManager = new SessionManager(properties,
    "localhost", 8000);

Create the table definition::

  TypeFactory ff = sessionManager.getTypeFactory();
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
   TableDefinition tableDefinition = sessionManager
     .newTableDefinition("employee", 1, employee_rowtype);
   tableDefinition.addIndex(2, "employee1.idx", new int[] { 0 }, true,
     true);
   tableDefinition.addIndex(3, "employee2.idx", new int[] { 2, 1 },
     false, false);
   tableDefinition.addIndex(4, "employee3.idx", new int[] { 5 },
     false, false);
   tableDefinition.addIndex(5, "employee4.idx", new int[] { 6 },
     false, false);

Create the table in the database::

   session.createTable(tableDefinition);

Insert a row::

   session.startTransaction(IsolationMode.READ_COMMITTED);
   boolean success = false;
   try {
    Table table = session.getTable(1);
    Row tableRow = table.getRow();
    tableRow.setInt(0, 1);
    tableRow.setString(1, "Joe");
    tableRow.setString(2, "Blogg");
    tableRow.setDate(5, getDOB(1930, 12, 31));
    tableRow.setString(6, "500.00");
    table.addRow(tableRow);

In the same transaction, scan the table::

    TableScan scan = table.openScan(0, null, false);
    try {
     Row row = scan.fetchNext();
     while (row != null) {
      System.out.println("Fetched row " + row);
      row.setString(6, "501.00");
      scan.updateCurrentRow(row);
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

Creating a SimpleDBM database
=============================

The database configuration is defined in a properties file. Example of the
properties file::

  logging.properties.file = classpath:simpledbm.logging.properties
  logging.properties.type = log4j
  network.server.host = localhost
  network.server.port = 8000
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
You also need to create a log4j config file, in this example, the server is being 
instructed to search for simpledbm.logging.properties file in the classpath.
An example of the logging properties file can be found in the SimpleDBM
distribution.

To create your new database, invoke SimpleDBM Network Server as follows:

  java -jar simpledbm-network-server-0.0.1-SNAPSHOT-jar-with-dependencies.jar create <properties file>

This will create an empty database in the location specified by the property
`storage.basePath`.

Starting a database
===================

Once a database has been created, it can be started using the following
commad:

  java -jar simpledbm-network-server-0.0.1-SNAPSHOT-jar-with-dependencies.jar open <properties file>

To stop the database server, simply press Control-C.

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


---------------
The Network API
---------------

SessionManager
==============

::

  /**
   * The SessionManager manages the connection to the SimpleDBM
   * Network Server, and initiates sessions used by the clients.
   * 
   * @author dibyendu majumdar
   */
  public abstract class SessionManager {

    /**
     * Obtains an instance of the SessionManager for the specified
     * connection parameters. The client should allow for the fact that the
     * returned instance may be a shared one.
     * 
     * @param properties
     * @param host
     * @param port
     * @return
     */
    public static SessionManager getSessionManager(Properties properties, String host, int port, int timeout);
    
    /**
     * Gets the TypeFactory associated with the database.
     * @return
     */
    public abstract TypeFactory getTypeFactory();
    
    /**
     * Gets the RowFactory for the database.
     * @return
     */
    public abstract RowFactory getRowFactory();
    
    /**
     * Creates a new TableDefinition.
     * @param name
     * @param containerId
     * @param rowType
     * @return
     */
    public abstract TableDefinition newTableDefinition(String name, int containerId,
    			TypeDescriptor[] rowType);
        
    /**
     * Starts a new session.
     * @return
     */
    public abstract Session openSession();
      
    /**
     * Gets the underlying connection object associated with this SessionManager.
     * <p>The connection object must be handled with care, as its correct
     * operation is vital to the client server communication.
     * @return
     */
    public abstract Connection getConnection();    
  }

Session
=======

::

  /**
   * A Session encapsulates an interactive session with the server.
   * Each session can only have one active transaction at any
   * point in time. Clients can open multiple simultaneous
   * sessions.
   */
  public interface Session {

    /**
     * Closes the session. If there is any outstanding transaction, it will
     * be aborted. Sessions should be closed by client applications when no 
     * longer required, as this will free up resources on the server.
     */
    public void close();
    
    /**
     * Starts a new transaction. In the context of a session, only one transaction can be active at
     * a point in time, hence if this method will fail if there is already an
     * active transaction.
     * @param isolationMode
     */
    public void startTransaction(IsolationMode isolationMode);
    
    /**
     * Commits the current transaction; an exception will be thrown if there is
     * no active transaction.
     */
    public void commit();
    
    /**
     * Aborts the current transaction; an exception will be thrown if there is
     * no active transaction
     */
    public void rollback();
    
    /**
     * Creates a table as specified. The table will be created using its own
     * transaction.
     * @param tableDefinition
     */
    public void createTable(TableDefinition tableDefinition);
    
    /**
     * Obtains a reference to the table. The Table container will be
     * locked in SHARED mode.
     * @param containerId
     * @return
     */
    public Table getTable(int containerId);
    
    /**
     * Gets the SessionManager that is managing this session.
     * @return
     */
    public SessionManager getSessionManager();
    
    /**
     * Gets the unique id associated with this session.
     * @return
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
       * Returns a new scan object.
       * @param indexno
       * @param startRow
       * @param forUpdate
       * @return
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
       * @param row
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
   * <p>
   * As rows are fetched, the scan maintains its position. The current
   * row may be updated or deleted. 
   * 
   * @author Dibyendu Majumdar
   */
  public interface TableScan {
    
    /**
     * Opens the scan, preparing for data to be fetched.
     * @return
     */
    public int open();
    
    /**
     * Fetches the next row. If EOF is reached, null will 
     * be returned.
     * @return
     */
    public Row fetchNext();
    
    /**
     * Updates the current row.
     * @param tableRow
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
     * @return
     */
    Session getSession();
  }
