.. -*- coding: utf-8 -*-

------------------
SimpleDBM Overview
------------------

:Author: Dibyendu Majumdar
:Contact: d dot majumdar at gmail dot com
:Date: 27 July 2014
:Version: 1.0.23
:Copyright: Copyright by Dibyendu Majumdar, 2005-2014

.. contents::

========
Overview
========

Introduction
============

SimpleDBM_ is a transactional database engine, written in Java. It has a
very small footprint and can be embedded in the address space of an
application. It provides a simple Java application programming interface (API), 
which can be learned very quickly.

.. _SimpleDBM: https://code.google.com/p/simpledbm/

Features
========

SimpleDBM implements the following features:

- *Transactional* - SimpleDBM fully supports ACID transactions. A STEAL and NO-FORCE buffer management strategy is used for transactions which is optimum for performance.
- *Multi-threaded* - SimpleDBM is multi-threaded and supports concurrent reads and writes of data.
- *Write Ahead Log* - SimpleDBM uses a write ahead log to ensure transaction recovery in the event of system crashes.
- *Lock based concurrency* - SimpleDBM uses row-level shared, update and exclusive locks to manage concurrency. 
- *Multiple Isolation Levels* - SimpleDBM supports read committed, repeatable read, and serializable isolation levels.
- *B-Tree Indexes* - SimpleDBM implements B-plus Tree indexes, that fully support concurrent reads, inserts and deletes. SimpleDBM B-Trees continually rebalance themselves, and do not suffer from fragmentation.
- *Tables* - SimpleDBM supports tables, but for maximum flexibility, treats table rows as blobs of data. Table rows can have any internal structure as you like, and can span multiple disk pages. Standard table rows with multiple columns are supported via add-on modules.
- *Latches and Locks* - SimpleDBM uses latches for internal consistency, and locks for concurrency. Latches are more efficient locking mechanisms that do not suffer from deadlocks.
- *Deadlock detection* - SimpleDBM has support for deadlock detection. A background thread periodically checks the lock table for deadlocks and aborts transactions to resolve deadlocks.
- *Network API* - From release 1.0.18 a network client server implementation is included that allows SimpleDBM servers to run standalone and remote clients to connect via TCP/IP. Only Java bindings available right now.

Non-Features
------------

- SimpleDBM is not an SQL engine. 
- There is no support for distributed transactions (XA).

Status
------

SimpleDBM is fully usable, and is available via Maven Central.
If you discover a bug, please report it - I will do my best to
fix any bugs. Enhancements are currently not being done as I have no
time available.

Getting Started
---------------

See `Getting Started <https://code.google.com/p/simpledbm/wiki/GettingStarted>`_ for instructions on how to start using SimpleDBM in your application.

Architecture
============

.. _System-R: http://www.mcjones.org/System_R/index.html

The core database engine of SimpleDBM is the RSS (named in honor of the
first IBM Relational Database prototype System-R_ Relational Storage
System). The RSS provides the underlying storage structures for
transactions, locking, b-trees etc. The functions 
of the RSS subsystem will be based upon the description of the System-R_ 
RSS component in [ASTRA-76]_.

   The Relational Storage Interface (RSI) is an internal interface
   which handles access to single tuples of base relations. This 
   interface and its supporting system, the Relational Storage 
   System (RSS), is actually a complete storage subsystem in that
   it manages devices, space allocation, storage buffers, transaction
   consistency and locking, deadlock detection, backout, transaction
   recovery, and system recovery. Furthermore, it maintains indexes
   on selected fields of base relations, and pointer chains across
   relations.  

The RSS API is however, somewhat low level for ordinary users. 
It is meant to be used by people interested in building their own 
Database Engines on top of RSS. 

To provides users with a simplified API, three additional modules are
available. 

The first one is the SimpleDBM TypeSystem module, which adds support
for typed data values and multi-attribute row objects.

The second module, the Database API. This module implements a high level 
Database API and uses the TypeSystem module on top of the RSS.

Finally there is the Network API that provides a Client / Server model.

Technology
----------

SimpleDBM is written in Java and uses features available since version 5.0
of this language.

Third party libraries
---------------------

SimpleDBM has no run-time dependency on external libraries as it uses
only out of the box Java 1.6 functionality. For test cases there is a
dependency on JUnit.

===============
Using SimpleDBM
===============

SimpleDBM is available in three levels of abstraction.

The `Network API`_ modules allow SimpleDBM to be deployed in a simple
Client / server configuration. 

The add-on modules SimpleDBM-Database and SimpleDBM Type-System provide
a high level API wth support for data dictionary, and the ability to create tables
with traditional row/column structure. Indexes can be associated with tables.
Details of how to use this API can be found in the document SimpleDBM `Database API`_.

The lower level RSS module works at the level of containers and arbitrary
types. The document named `RSS User Manual`_ provides instructions on how to develop
using the RSS. Note that this is for advanced users who want to implement their
own type system and data dictionary.

====================
Developing SimpleDBM
====================

The instructions in this section are for those who wish to develop SimpleDBM.

Obtaining SimpleDBM
===================
SimpleDBM source code can be obtained from the `SimpleDBM Project site`_. Source code is maintained in a Mercurial repository, so you will 
need a Mercurial client on your computer.

.. _Network API: https://simpledbm.readthedocs.org/en/latest/network-api.html
.. _Database API: https://simpledbm.readthedocs.org/en/latest/database-api.html
.. _RSS User Manual: https://simpledbm.readthedocs.org/en/latest/usermanual.html
.. _SimpleDBM project site: http://simpledbm.googlecode.com/

The SimpleDBM SCM repository is organized as follows:

::

 trunk  --+--- simpledbm-rss   	      This contains the core DBMS engine
          |
          +--- simpledbm-common       This contains basic utilities that are
          |                           shared by all projects.
          |
          +--- simpledbm-typesystem   This contains a simple typesystem
          |                           that can be used with SimpleDBM.
          |
          +--- simpledbm-database     This contains a higher level DB
          |                           API that makes life easier for
          |                           users. It uses the typesystem
          |                           component.
          |
          +--- simpledbm-samples      This contains some sample programs
          |                           that demonstrate how to use SimpleDBM.
          |
          +--- simpledbm-docs         Contains the documentation sources.                           

Under each of the top-level folders, there is the following structure.

::

 --+--- code            This is where the source code is.
   |
   +--- docs            This folder contains documents.
   |
   +--- site            This folder contains web site contents.

Some of these folders may be empty if no content has been created.

In the code sub-directory, there is a top-level directory for each project.

Mercurial URLs
--------------

Here are the Mercurial URLs for the various SimpleDBM sub-systems.

Base URL
  http://simpledbm.googlecode.com/hg/

SimpleDBM-Common
  .../simpledbm-common/code/simpledbm-common

SimpleDBM-RSS
  .../simpledbm-rss/code/simpledbm-rss

SimpleDBM-TypeSystem
  .../simpledbm-typesystem/code/simpledbm-typesystem

SimpleDBM-Database
  .../simpledbm-database/code/simpledbm-database

TupleDemo sample
  .../simpledbm-samples/code/tupledemo

B-TreeDemo sample
  .../simpledbm-samples/code/btreedemo

If you are a committer, you need to use ``https`` instead of ``http``.

Build Instructions
==================

Pre-requisites
--------------

SimpleDBM uses Maven_ for build management. You will need to obtain a
copy of Maven 3. Install Maven and set up your PATH so that Maven can be
executed by typing the following command.

:: 
  
  mvn

.. _Maven: http://maven.apache.org.

SimpleDBM development is being done using Eclipse. You can use any IDE
of your choice, but you may need to find ways of converting the maven
projects to the format recognized by your IDE.

You will need a Mercurial client in order to checkout the code for
SimpleDBM. 

SimpleDBM requires Java SE 1.6 or above. 

Instructions for Eclipse
------------------------
The following instructions are for the simpledbm-rss project.
However, the same instructions apply for the other projects.

1. Use the Mercurial command line tools to create a local clone of the
   SimpleDBM Repository::
    
    hg clone http://simpledbm.googlecode.com/hg simpledbm

2. Import the SimpleDBM Maven projects into Eclipse. The parent pom file
   is in the ``build`` folder. This is a multi-module pom file and will generate
   sub projects below it.

Maven commands 
--------------
You can also compile, test and do other operations using maven commands.
The following maven commands are commonly used.

To run the test cases.

::

  cd build
  mvn test

To create the package and install it in the local repository.

::

  mvn install

Please visit the SimpleDBM project Wiki pages for additional platform
specific instructions.

Building releases
=================

SimpleDBM releases are published to Maven Central. Please contribute your
changes to SimpleDBM maintainer (admin@simpledbm.org) as releases can only 
be performed by the maintainer.

============================
Coding and Design Principles
============================

Modular design
==============

SimpleDBM RSS is made up of several modules. Each module implements a
particular component, and is contained in its own package.

Each module has a public API, which is specified via a set of Java
interfaces. Classes are generally not used as part of the public API,
though there are a few exceptional cases.

To make the modules reusable and as independent of each other as
possible, the interface of a module is deliberately specified in
general terms. Where possible, direct dependence between modules is
avoided. The only permissible way for one module to interact with 
another is to go via the public interfaces of the other module. 
Modules are not allowed to depend upon implementation specifics of 
other modules.

A strict rule is that two modules cannot have cyclic dependency.
Module dependencies are one-way only, higher level modules depend
upon lower level modules. This is illustrated below.

.. image:: images/component-model.png
   :scale: 30

SimpleDBM uses constructor based dependency injection to link
modules. It is being designed in such a way that a third-party IoC
(Inversion of Control) container may be used to manage the
dependencies.

Java coding standards
=====================

Where possible, classes are made immutable. This helps in 
improving the robustness of the system. The serialization mechanism
used by SimpleDBM is designed to work with immutable objects.

In the interest of concurrency, fine-grained locking is used as 
opposed to coarse-grained synchronization. This makes the code complex
in some cases, as careful ordering of locks is required for deadlock
avoidance. Also, the correctness of synchronization logic is of 
paramount importance.

Unchecked exceptions are used throughout. Due to the nature of 
unchecked exceptions, the code that throws the exception has the 
responsibility of logging an error message at the point where the
exception is thrown. This ensures that even if the exception is not
caught by the client, an error message will be logged to indicate 
the nature of the error.

All error messages are given unique error codes.

The code relies upon the efficiency of modern garbage collectors
and does not attempt to manage memory. Rather than
using object pools, SimpleDBM encourages the use of short-lived
objects, on the basis that this aids the garbage collector in
reclaiming space more quickly. The aim is to keep permanently
occupied memory to a low level.

JUnit based test cases are being added constantly to improve the
test coverage. Simple code coverage statistics are not a good indicator of the
usefulness of test cases, due to the multi-threaded nature of most
SimpleDBM components. Where possible, test cases are created to simulate
specific thread interactions, covering common scenarios. 

Particular attention is paid to cleaning up of resources. To ensure
that resources are cleaned up during normal as well as exceptional
circumstances, finally blocks are used.

Debug messages are used liberally - and are executed conditionally
so that if debug is switched off, there is minimal impact on
performance.

A special Trace module is used to capture runtime trace. This module
is designed to be lock-free, and is very low overhead, so that trace
can be collected with negligible overhead. This feature is still being
implemented across modules; the intention is that when fatal errors
occur, the last 5000 trace messages will be dumped to help debug the
error condition.

=============
Documentation
=============

Most of the documentation for SimpleDBM is written in reStructuredText.
HTML and PDF versions are generated from the source documents.
There is a generous amount of comments in the source code as well. 

Being an educational project, producing good documentation is high
priority.

The design of most modules is based upon published research. References
are provided in appropriate places, both in this document, and in the
source code. This acts as another source of information.

Following documents are recommended as starting points:

  * `SimpleDBM Overview <https://simpledbm.readthedocs.org/en/latest/overview.html>`_ - provides an overview of SimpleDBM
  * `Database API`_ - describes the Database API
  * `SimpleDBM TypeSystem <https://simpledbm.readthedocs.org/en/latest/typesystem.html>`_ - useful if you want to know more about the type system

For advanced stuff, read:

  * `SimpleDBM RSS User Manual <https://simpledbm.readthedocs.org/en/latest/usermanual.html>`_ - describes the low level API of RSS
  * `SimpleDBM RSS Developers Guide <https://simpledbm.readthedocs.org/en/latest/developerguide.html>`_ - covers internals of RSS, the SimpleDBM database engine
  * `BTree Space Management <http://simpledbm.googlecode.com/files/btree-space-management-1.0.pdf>`_ - describes some implementation issues with BTree space management

JavaDoc (these are slightly older versions) for the main projects:

  * `Database API JavaDoc <http://simpledbm.googlecode.com/files/simpledbm-database-1.0.11-javadoc.jar>`_ - contains the JavaDoc for the SimpleDBM Database API
  * `TypeSystem JavaDoc <http://simpledbm.googlecode.com/files/simpledbm-typesystem-1.0.10-javadoc.jar>`_ - contains JavaDoc for the TypeSystem.
  * `SimpleDBM RSS JavaDoc <http://simpledbm.googlecode.com/files/simpledbm-rss-1.0.15-SNAPSHOT-javadoc.jar>`_ - provides JavaDoc for the RSS component.


.. [ASTRA-76] M.M.Astrahan, M.W.Blasgen, D.D.Chamberlin,
   K.P.Eswaran, J.N.Gray, P.P.Griffiths, W.F.King, R.A.Lorie,
   P.R.McJones, J.W.Mehl, G.R.Putzolu, I.L.Traiger, B.W.Wade
   AND V.Watson. System R: Relational Approach to Database
   Management, ACM, Copyright 1976, ACM Transactions on
   Database Systems, Vol 1, No. 2, June 1976, Pages 97-137.
