/***
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program; if not, write to the Free Software
 *    Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *
 *    Project: www.simpledbm.org
 *    Author : Dibyendu Majumdar
 *    Email  : d dot majumdar at gmail dot com ignore
 */

/**
 * <p>Specifies the interface to the Write Ahead Logging (WAL) system.</p>
 * <h2>Introduction</h2>
 * <p>The Write Ahead Log plays a crucial role in a DBMS. It provides the
 * basis for recoverability. It is also a critical part of the system that
 * has a massive impact on performance of an OLTP system.</p>
 * <p>Conceptually, the Log can be thought of as an ever growing sequential
 * file. In the form of Log Records, the Log contains a history of all
 * changes made to the database. Each Log Record is uniquely identified by
 * a number called the Log Sequence Number (LSN). The LSN is designed in
 * such a way that given an LSN, the system can locate the corresponding
 * Log Record quickly. LSNs are assigned in strict ascending order
 * (monotonicity). This is an important property when it comes to recovery.</p>
 * <p>During the progress of a Transaction, the a DBMS records in the Log
 * all the changes made by the transaction. The Log records can be used to
 * recover the system if there is a failure, or they can be used to undo
 * the changes made by a transaction.</p>
 * <p>Initially, Log Records are stored in memory. They are flushed to disk
 * during transaction commits, and also during checkpoints. In the event of
 * a crash, it is possible to lose the log records that were not flushed to
 * disk. This does not cause a problem, however, because by definition
 * these log records must correspond to changes made by incomplete
 * transactions. Also, the WAL protocol (described below) ensures that such
 * Log records do contain changes that have already been persisted within
 * the database.</p>
 * <h2>Write Ahead Log (WAL) Protocol</h2>
 * <p>The WAL protocol requires the following conditions to hold true:</p>
 * <ol>
 * <li>All changes made by a transaction must be saved to the Log and the
 * Log must be flushed to disk <em>before</em> the transaction is
 * committed.</li>
 * <li>A database buffer page may not be modified until its modifications
 * have been logged. A buffer page may not be saved to disk until all its
 * associated log records have been saved to disk.</li>
 * <li>While the buffer page is being modified and the Log is being
 * updated, an Exclusive latch (a type of fast lock) must be held on the
 * page to ensure that order in which changes are recorded in the Log
 * correspond to the order in which they were made.</li>
 * </ol>
 * <p>Consequences of above rules are:</p>
 * <ol>
 * <li>If a Log Record was not saved to disk, it can be safely ignored,
 * because any changes contained in it are guaranteed to belong to
 * uncommitted transactions. Also, such Log Records cannot represent
 * changes that have been made persistent in the database.</li>
 * <li>Log records represent changes to the system in the correct order.
 * The latching protocol ensures that if two Log records represent changes
 * to the same Page, then the ordering of these records reflects the order
 * in which the changes were made to the page.</li>
 * </ol>
 * <h2>Advantages of WAL</h2>
 * <p>Typically, in an OLTP system, updates tend to be random and can
 * affect different parts of the disk at a point in time. In comparison,
 * writes to the Log are always sequential. If it were necessary to flush
 * all changes made by the DBMS to disk at commit time, it would have a
 * massive impact on performance because of the randomness of the disk
 * writes. However, in a WAL system, only the Log needs to be flushed to
 * disk at Commit. Thus, the Log has the effect of transforming random
 * writes into serial writes, thereby improving performance significantly.</p>
 * <h2>Design Issues</h2>
 * <p>There are several design decisions that must be made when
 * implementing a Write Ahead Log.</p>
 * <ol>
 * <li>Should the Log Manager share the page buffering system used by the
 * DBMS to manage IO to data files? The database page buffering system
 * usually operates for fixed size pages, whereas log records are
 * typically variable length. Using a page buffering system would require
 * mapping of log records to pages. There is also the chicken and egg
 * problem. Changes made to disk pages are supposed to be logged in the
 * write ahead log. Having the log system depend on the page buffer system
 * would introduces a cyclical dependency, which is best to avoid. Last,
 * but not the least, log systems usually require very high io
 * performance, and can take advantage of the fact that log writes are
 * always sequential. A custom algorithm for handling log writes is likely
 * to achieve better performance than the more generic solution in page
 * buffering systems.</li>
 * <li>Should Log Records be allowed to span multiple log files? It is
 * easier to keep things simple and insist that the largest of log records
 * must fit into an individual log file. Clients that require larger log
 * records can always split them into multiple records.</li>
 * <li>Should log files be allocated as and when needed? There is often a
 * middle ground; a fixed set of online log files can be permanently
 * allocated. Having a fixed set of online log files makes the system more
 * reliable, because there is no need to allocate a file when writing to
 * the Log. New files can be allocated when archiving log files. This
 * ensures that even if the log archival fails, the main log files will
 * remain intact.</li>
 * <li>What is the level of concurrency that should be supported in the
 * log system? Using fine-grained locking increases the complexity of the
 * code enormously, but should result in greater scalability. Concurrency
 * solution can take advantage of the fact that log writes always occur at
 * the end of the log.</li>
 * </ol>
 */
package org.simpledbm.rss.api.wal;

