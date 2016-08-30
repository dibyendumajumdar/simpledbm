SimpleDBM is an Open Source Transactional Database Engine in Java. It has
a very small footprint and can be embedded in the address space of an
application. It provides a simple programming API, which can be learned
very quickly. A simple network API is available for remote access.

## Features ##
SimpleDBM has the following features:
  1. **Transactional** - SimpleDBM fully supports ACID transactions. SimpleDBM uses a STEAL/NO-FORCE buffer management strategy for transactions.
  1. **Multi-threaded** - SimpleDBM is multi-threaded and supports concurrent reads and writes of data.
  1. **Write Ahead Log** - SimpleDBM uses a write ahead log to ensure transaction recovery in the event of system crashes.
  1. **Lock based concurrency** - SimpleDBM uses shared, update and exclusive row locks to manage concurrency.
  1. **Multiple Isolation Levels** - SimpleDBM supports read-committed, repeatable-read, and serializable isolation levels.
  1. **B-Tree Indexes** - SimpleDBM implements B-plus Tree indexes, that fully support concurrent reads, inserts and deletes. SimpleDBM B-Trees continually re-balance themselves, and do not suffer from fragmentation.
  1. **Tables** - SimpleDBM supports tables, but for maximum flexibility, treats table rows as blobs of data. Table rows can have any internal structure as you like, and can span multiple disk pages. Standard table rows with multiple columns are supported via add-on modules.
  1. **Latches and Locks** - SimpleDBM uses latches for internal consistency, and locks for concurrency. Latches are more efficient locking mechanisms that do not suffer from deadlocks.
  1. **Deadlock detection** - SimpleDBM has support for deadlock detection. A background thread periodically checks the lock table for deadlocks and aborts transactions to resolve deadlocks.
  1. **Network API** - From release 1.0.18 a network client server implementation is included that allows SimpleDBM servers to run standalone and remote clients to connect via TCP/IP. Only Java bindings available right now.

## Non Features ##
  1. SimpleDBM does not suport SQL.
  1. There is no support for distributed transactions (XA) yet.

## Status ##
SimpleDBM is available via Maven Central. The latest release is 1.0.23. If you discover bugs please report as I will do my best to fix bugs. Enhancements are currently on hold due to lack of time.

## News ##
  * 1 Aug 2014 - SimpleDBM 1.0.23 uploaded to maven central. From this release the primary license is Apache V2.0 - but GPL and LGPL continue to be options.
  * 22 May 2013 - SimpleDBM 1.0.22 uploaded to maven central
  * 3 June 2012 - SimpleDBM 1.0.19-ALPHA released. This is a bug fix release.
  * 19 April 2010 - SimpleDBM 1.0.18-ALPHA released. This contains a first implementation of the network client server.

## License ##
SimpleDBM license has been changed to Apache License 2.0 from 1.0.23 onwards.

# Getting Started with SimpleDBM #

To start developing code with SimpleDBM, you need to include following maven dependencies:

## Embedded Database ##
This option is where you want the database engine to be inside your application, deployed as part of the same JVM.

```
        <dependency>
            <groupId>org.simpledbm</groupId>
            <artifactId>simpledbm-common</artifactId>
            <version>1.0.23</version>
        </dependency>
        <dependency>
            <groupId>org.simpledbm</groupId>
            <artifactId>simpledbm-rss</artifactId>
            <version>1.0.23</version>
        </dependency>
        <dependency>
            <groupId>org.simpledbm</groupId>
            <artifactId>simpledbm-typesystem</artifactId>
            <version>1.0.23</version>
        </dependency>
        <dependency>
            <groupId>org.simpledbm</groupId>
            <artifactId>simpledbm-database</artifactId>
            <version>1.0.23</version>
        </dependency>
```

See the Database API documentation below.

## Network Server ##
The Network Server allows a client / server deployment.
Instructions to follow...

# Documentation #
For documentation, I recommend you start with:

  * [SimpleDBM Overview](http://simpledbm.readthedocs.io/en/latest/overview.html) - provides an overview of SimpleDBM
  * [SimpleDBM Network API](http://simpledbm.readthedocs.io/en/latest/network-api.html) - describes the Network API for client/server implementation
  * [SimpleDBM Database API](http://simpledbm.readthedocs.io/en/latest/database-api.html) - describes the Database API for embedded use
  * [SimpleDBM TypeSystem](http://simpledbm.readthedocs.io/en/latest/typesystem.html) - useful if you want to know more about the type system

For advanced stuff, read:

  * [SimpleDBM RSS User's Manual](http://simpledbm.readthedocs.io/en/latest/usermanual.html) - describes the low level API of RSS
  * [SimpleDBM Developer's Guide](http://simpledbm.readthedocs.io/en/latest/developerguide.html) - covers internals of RSS, the SimpleDBM database engine
  * [BTree Space Management](https://github.com/dibyendumajumdar/simpledbm/blob/master/docs/btree-space-management.rst) - describes some implementation issues with BTree space management

You can read the [SimpleDBM Blog](http://simpledbm.blogspot.com/) and other papers available in the downloads section. If you are interested in development, you should also read the literature referred to in the [Bibliography](https://github.com/dibyendumajumdar/simpledbm/wiki/Bibliography).

If you find bugs, please raise Issues. You can also post questions in the [discussion group](http://groups.google.com/group/simpledbm).
