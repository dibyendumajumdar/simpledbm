# Introduction #

This short HOWTO describes where to start with SimpleDBM.

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

  * [SimpleDBM Overview](https://simpledbm.readthedocs.org/en/latest/overview.html) - provides an overview of SimpleDBM
  * [SimpleDBM Network API](https://simpledbm.readthedocs.org/en/latest/network-api.html) - describes the Network API for client/server implementation
  * [SimpleDBM Database API](https://simpledbm.readthedocs.org/en/latest/database-api.html) - describes the Database API for embedded use
  * [SimpleDBM TypeSystem](https://simpledbm.readthedocs.org/en/latest/typesystem.html) - useful if you want to know more about the type system

For advanced stuff, read:

  * [SimpleDBM RSS User's Manual](https://simpledbm.readthedocs.org/en/latest/usermanual.html) - describes the low level API of RSS
  * [SimpleDBM Developer's Guide](https://simpledbm.readthedocs.org/en/latest/developerguide.html) - covers internals of RSS, the SimpleDBM database engine
  * [BTree Space Management (pdf)](http://simpledbm.googlecode.com/files/btree-space-management-1.0.pdf) - describes some implementation issues with BTree space management

JavaDoc for the main projects:

  * [Database API JavaDoc](http://simpledbm.googlecode.com/files/simpledbm-database-1.0.11-javadoc.jar) - contains the JavaDoc for the SimpleDBM Database API
  * [TypeSystem JavaDoc](http://simpledbm.googlecode.com/files/simpledbm-typesystem-1.0.10-javadoc.jar) - contains JavaDoc for the TypeSystem.
  * [SimpleDBM RSS JavaDoc](http://simpledbm.googlecode.com/files/simpledbm-rss-1.0.15-SNAPSHOT-javadoc.jar) - provides JavaDoc for the RSS component.

Finally, you can read the [SimpleDBM Blog](http://simpledbm.blogspot.com/) and other papers available in the downloads section. If you are interested in development, you should also read the literature referred to in the Bibliography.

If you find bugs, please raise Issues. You can also post questions in the [discussion group](http://groups.google.com/group/simpledbm).

See ProjectNews for updates on SimpleDBM.