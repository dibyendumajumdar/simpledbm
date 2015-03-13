# Project News #
  * 01-Aug-2014:
    * [issue 96](https://code.google.com/p/simpledbm/issues/detail?id=96): Apply Apache V2.0 license
    * [issue 97](https://code.google.com/p/simpledbm/issues/detail?id=97): Merge v2 branch updates
    * [issue 98](https://code.google.com/p/simpledbm/issues/detail?id=98): Change minimum JDK version to 1.6
  * 03-Jun-12: Network Client Server release 1.0.19-ALPHA
    * [Issue 93](https://code.google.com/p/simpledbm/issues/detail?id=93): The timer code has a bug whereby in the no timeout scenario it can still timeout occasionally if the underlying park() times out
    * [Issue 94](https://code.google.com/p/simpledbm/issues/detail?id=94): Maven build of SimpleDBM-RSS broken due to change in forkMode
    * [Issue 95](https://code.google.com/p/simpledbm/issues/detail?id=95): Maven pom files do not have a relative path to the parent pom
  * 19-Apr-10: Network Client Server release 1.0.18-ALPHA
    * [Issue 47](https://code.google.com/p/simpledbm/issues/detail?id=47): Create a network client server
    * [Issue 90](https://code.google.com/p/simpledbm/issues/detail?id=90): Implement a shared thread pool
    * [Issue 91](https://code.google.com/p/simpledbm/issues/detail?id=91): Create a sample application to demonstrate/test the network server
  * 07-Mar-10: Bug fix releases
    * [Issue 89](https://code.google.com/p/simpledbm/issues/detail?id=89): Unique index allows duplicate key insert
    * [Issue 88](https://code.google.com/p/simpledbm/issues/detail?id=88): Enhance exceptions to use numeric error codes
  * 18-Oct-09: New releases of all the modules - SimpleDBM RSS 1.0.16, Common 1.0.2, TypeSystem 1.0.12, and Database 1.0.13, containing following changes:
    * [Issue 84](https://code.google.com/p/simpledbm/issues/detail?id=84): HTML documentation created using sphinx from RST sources
    * [Issue 56](https://code.google.com/p/simpledbm/issues/detail?id=56): Documented settings that impact performance
    * [Issue 67](https://code.google.com/p/simpledbm/issues/detail?id=67): Exception stack traces are not captured when the error is initially logged
    * [Issue 69](https://code.google.com/p/simpledbm/issues/detail?id=69): Create a platform module to encapsulate the base utilities such as logging, tracing, exception handling, etc.
    * [Issue 73](https://code.google.com/p/simpledbm/issues/detail?id=73): Break out the common utility stuff out of the RSS module and make Type-System independent of RSS
    * [Issue 75](https://code.google.com/p/simpledbm/issues/detail?id=75): Documented the usage of lock file and manual recovery step for a restart
    * [Issue 76](https://code.google.com/p/simpledbm/issues/detail?id=76): Implemented table drop functionality
    * [Issue 77](https://code.google.com/p/simpledbm/issues/detail?id=77): Migrated version control from SVN to Mercurial
    * [Issue 78](https://code.google.com/p/simpledbm/issues/detail?id=78): Updated SCM details in Maven
    * [Issue 79](https://code.google.com/p/simpledbm/issues/detail?id=79): Added Eclipse project files to version control
    * [Issue 80](https://code.google.com/p/simpledbm/issues/detail?id=80): Fixed undo of container create operations to remove deleted containers physically
    * [Issue 81](https://code.google.com/p/simpledbm/issues/detail?id=81): Ensured that during undo of table create the table definition is deleted
  * 28-Jun-09: [Issue 77](https://code.google.com/p/simpledbm/issues/detail?id=77): Migrated the source code repository from Subversion to Mercurial.
  * 31-May-09: [Issue 74](https://code.google.com/p/simpledbm/issues/detail?id=74): GNU Classpath exception added to the license to enable use of SimpleDBM in combined works without requiring the entire work to be licensed under GPL.
  * 13-Apr-09: SimpleDBM RSS 1.0.14 BETA release. Following issues were fixed:
    * [Issue 71](https://code.google.com/p/simpledbm/issues/detail?id=71): BTree: Fixed defect when scanning a tree.
  * 07-Apr-09: SimpleDBM RSS 1.0.12 BETA release. Following issues were fixed:
    * [Issue 66](https://code.google.com/p/simpledbm/issues/detail?id=66): BufferManager: Fixed page replacement logic to always evict the LRU page even if it is dirty.
    * [Issue 64](https://code.google.com/p/simpledbm/issues/detail?id=64): BTree: Fetch fails when the search key is greater than all keys in the tree
    * [Issue 59](https://code.google.com/p/simpledbm/issues/detail?id=59): BTree: Improve redistribute keys in BTree implementation
    * [Issue 58](https://code.google.com/p/simpledbm/issues/detail?id=58): General: Change the logger names so that the log messages have better module identification
    * [Issue 70](https://code.google.com/p/simpledbm/issues/detail?id=70): BTree: Removed assertion that looks incorrect after the new redistribute keys method.
  * 07-Apr-09: SimpleDBM Database API 1.0.11 EXPERIMENTAL release.
  * 07-Apr-09: SimpleDBM TypeSystem 1.0.10 EXPERIMENTAL release.
  * 17-Aug-08: SimpleDBM Database API 1.0.10 EXPERIMENTAL release.
  * 17-Aug-08: SimpleDBM TypeSystem 1.0.9 EXPERIMENTAL release.
  * 17-Aug-08: SimpleDBM RSS 1.0.11 BETA release.
  * 30-Jul-08: SimpleDBM Database API 1.0.9 EXPERIMENTAL release.
  * 30-Jul-08: SimpleDBM RSS 1.0.10 BETA released. Contains several critical fixes.
  * 11-Jul-08: SimpleDBM TypeSystem 1.0.8a EXPERIMENTAL release. Following issues fixed:
    * [Issue 24](https://code.google.com/p/simpledbm/issues/detail?id=24) DateTimeValue comparisons are not working correctly.
  * 11-Jul-08: SimpleDBM Database API 1.0.8a EXPERIMENTAL release. Following issued fixed:
    * [Issue 23](https://code.google.com/p/simpledbm/issues/detail?id=23) TableScan is not updating secondary indexes when a row is updated.
  * 09-Jul-08: SimpleDBM Database API 1.0.8 EXPERIMENTAL release. Contains a high level API that supports a simple data dictionary. Tables with multi-attribute rows and multiple indexes are supported.
  * 09-Jul-08: SimpleDBM TypeSystem 1.0.8 EXPERIMENTAL release. Contains a simple typesystem for SimpleDBM.
  * 09-Jul-08: SimpleDBM RSS 1.0.8 BETA released. Minor enhancements to support the new database API.
  * 08-Nov-07: SimpleDBM RSS 1.0.7 BETA released. Contains critical bug fixes.
  * 03-Nov-07: SimpleDBM RSS 1.0.6 BETA released. This release contains important bug fixes. There is now support for Log4J. User Manual has been updated. Also available in this release is the Developer's Guide, which has had a major overhaul. All documents have been converted to [ReStructuredText](http://docutils.sourceforge.net/rst.html) format.
  * 14-Sep-07: SimpleDBM RSS 1.0.5 BETA released
  * 10-Jun-07: SimpleDBM User Manual published
  * 10-Jun-07: SimpleDBM RSS 1.0.2 BETA released
  * 03-Jun-07: SimpleDBM 1.0.1 BETA released.
  * 02-Jun-07: SimpleDBM 1.0.0 BETA released.
  * 31-Oct-06: SimpleDBM source code repository has a new [home](http://simpledbm.googlecode.com).
  * 31-Oct-06: Current builds of SimpleDBM use Maven 2 instead of ant.
  * 02-Jun-06: SimpleDBM project's hosting is moving to ObjectWeb.
  * 06-Apr-06: Build number 0.58 contains initial implementation the RSS Server component.
  * 20-Feb-06: Build number 0.55 is now available. This contains an implementation of Tuple Scans which makes the Tuple Manager feature complete.
  * 27-Jan-06: Latest build contains fixes for a few bugs, implementation of missing features in the Transaction Manager and Log Manager modules that were on the TODO list for a while, and some refactoring of code.
  * 14-Jan-06: First draft of a paper on Multi-Version Concurrency implementations is available here.
  * 29-Dec-05: Completed first cut implementation of Tuple Manager. See [WEBLOG](http://simpledbm.blogspot.com) for a discussion of this module.
  * 04-Dec-05: Added a Demo program for BTree module that allows an interactive session involving two threads. Here is a screen shot.
  * 25-Nov-05: Bug fixes, test cases, refactoring and documentation updates are the theme for this month.
  * 09-Nov-05: Documentation page added.
  * 04-Nov-05: BTree scans implemented. The basic functionality of BTree indexes is now available.
  * 28-Oct-05: BTree Insert Key operation implemented.
  * 06-Oct-05: Wrote a short paper on some of the [space management issues](http://simpledbm.googlecode.com/files/btree-space-management-1.0.pdf) in B-link trees.
  * 18-Sep-05: Work on BTree module is starting today. The implementation will be based upon the algorithm for B-link tree published in Concurrency control and recovery for balanced B-link trees, by Ibrahim Jaluta, Seppo Sippu and Eljas Soisalon-Soininen.
  * 17-Sep-05: In preparation for the BTree module, the Buffer Manager module has been enhanced to support additional (update) latch mode. A new Latch implementation added to support this requirement. Also, SlottedPage Manager module implemented to handle the common requirements of manipulating slots and items within a page.
  * 16-Sep-05: Port from DM1 code completed. A lot of refactoring and bug fixes have been done in the process. The DM1 code is now truly defunct.
  * 07-Sep-05: Most of the Space Manager module is done. Testing should be completed by this week.
  * 28-Aug-05: Transaction Manager implementation is now available. New milestone release available here.
  * 23-Aug-05: Added a Latch implementation and started work on Transaction Manager
  * 19-Aug-05: Source Code checked into UNSTABLE\_V100 branch.
  * 19-Aug-05: Initial port of Buffer Manager.
  * 14-Aug-05: There is now a [WEBLOG](http://simpledbm.blogspot.com) for the project.
  * 28-Jul-05: Code for Lock Manager ported.
  * 09-Jul-05: Code for Log Manager ported.
  * 10-Jun-05: Project started.