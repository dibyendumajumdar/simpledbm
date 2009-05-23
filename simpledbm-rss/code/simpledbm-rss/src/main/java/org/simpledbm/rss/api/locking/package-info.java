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
 *    Linking this library statically or dynamically with other modules 
 *    is making a combined work based on this library. Thus, the terms and
 *    conditions of the GNU General Public License cover the whole
 *    combination.
 *    
 *    As a special exception, the copyright holders of this library give 
 *    you permission to link this library with independent modules to 
 *    produce an executable, regardless of the license terms of these 
 *    independent modules, and to copy and distribute the resulting 
 *    executable under terms of your choice, provided that you also meet, 
 *    for each linked independent module, the terms and conditions of the 
 *    license of that module.  An independent module is a module which 
 *    is not derived from or based on this library.  If you modify this 
 *    library, you may extend this exception to your version of the 
 *    library, but you are not obligated to do so.  If you do not wish 
 *    to do so, delete this exception statement from your version.
 *
 *    Project: www.simpledbm.org
 *    Author : Dibyendu Majumdar
 *    Email  : d dot majumdar at gmail dot com ignore
 */

/**
 * <p>Defines the interface to the Locking sub-system.</p>
 * <h2>Introduction</h2>
 * <p>All multi-user transactional systems use some form of locking to
 * ensure that concurrent transactions do not conflict with each other.
 * Depending upon the level of consistency guaranteed by the transactional
 * system the number and type of locks used can vary.</p>
 * <p>In a single user system, no locking is needed. Transaction are
 * automatically consistent, as only one transaction can execute at any
 * point in time.</p>
 * <p>In multi-user systems, transactions must be allowed to proceed
 * concurrently if reasonable performance is to be obtained. However, this
 * means that unless some form of locking is used, data consistency
 * problems will arise. For example, if two transactions update the same
 * record at the same time, one of the updates may be lost.</p>
 * <h2>Locking Basics</h2>
 * <p>To prevent this sort of thing from happening, each transaction must
 * lock the data that it updates or reads. A lock is a mechanism by which
 * access to the record is restricted to the transaction that owns the
 * lock. Furthermore, a lock restricts the type of operation that is
 * permitted to occur. For example, a Shared lock can be owned by multiple
 * transactions concurrently and allows read operations. An Exclusive lock
 * permits both read and write operations but can only be granted to one
 * transaction at any point on time. Moreover Shared locks and Exclusive
 * locks are incompatible; this means that if a Shared Lock is held by a
 * transaction on a record, another transaction cannot obtain an Exclusive
 * lock on the same record, and vice-versa.</p>
 * <h2>Two-Phase Locking and Repeatable Read Isolation Level</h2>
 * <p>Not only must a record be locked when it is updated, the transaction
 * must hold the lock until the transaction is committed or aborted. This
 * strategy leads to the basic rule of two-phase locking, which requires
 * that a transaction must manage its locks in two distinct phases. In the
 * first phase, the transaction is permitted to acquire locks, but cannot
 * release any locks. The first phase lasts right upto the moment the
 * transaction is completed, i.e., either committed or aborted. In the
 * second phase, when the transaction is committed or aborted, all locks
 * are released. No further locks can be acquired in this phase. Strict two
 * phase locking ensures that despite concurrent running of transactions,
 * each transaction has the appearance of running in isolation. Strict
 * two-phase locking strategy provides a level of consistency called
 * Repeatable Read.</p>
 * <h2>Read Committed Isolation Level</h2>
 * <p>This basic strategy can be modified to obtain greater concurrency at
 * the cost of data consistency. For example, read locks can be released
 * early to allow other transactions to read data. While this increases
 * concurrency, it does mean that reads are not repeatable, because the
 * original transaction may find that the data it read previously has been
 * modified by the time it is read a second time. This level of consistency
 * is known as Read Committed.</p>
 * <h2>Serializable Isolation Level</h2>
 * <p>Although the Repeatable Read level of consistency prevents data that
 * has been read by one transaction from being modified by another, it does
 * not prevent the problem of phantom reads, which occurs when new records
 * are inserted. For example, if a range of records is read twice by the
 * same transaction, and another transaction has inserted new records in
 * the time interval between the two reads, then the second read will
 * encounter records that did not appear the first time. To prevent this
 * type of phantom reads from ocurring, locking has to be made even more
 * comprehensive. Rather than locking one record, certain operations need
 * to lock entire ranges of records, even non-existent ones. This is
 * typically achieved using a logical convention; a lock on a particular
 * data item represents not only a lock on that data, but also the range of
 * data upto and including the data item being locked. For example, if
 * there are two records A and C, then a lock on C would encompass the
 * entire range of data between A and C, excluding A, but including and
 * upto C.</p>
 * <h2>Design choices</h2>
 * <p>The Locking subsystem specified here requires that locks should be
 * implemented independently of the objects being locked. 
 * In order for locking to work, all participants must agree to agree to
 * use the locking system and abide by the rules.</p>
 * <p>Another design constraint is that the interface is geared towards a
 * memory based implementation. This places a constraint on the number of
 * locks that can be held within the system, because a large number of
 * locks would require a prohibitively large amount of memory.</p>
 * <p>Some database systems, Oracle, in particular, use markers within the
 * databases disk pages to represent locks. A lock byte is used, for
 * instance, to denote whether a row is locked or not. The advantage of
 * Oracle's approach is that there are no constraints on the number of
 * locks the system can handle. The disadvantage is that the lock status is
 * maintained in persistent storage, therefore changing the lock status can
 * make a page dirty. Oracle overcomes this issue in two ways. Firstly, it
 * uses a multi-version system that does not required read locks. Thus
 * locks are used only for updates, and since updates cause database
 * pages to be touched anyway, using a lock status byte does not pose a
 * problem. Secondly, Oracle avoids updating the lock status byte when 
 * locks are released, by using information about the
 * transaction status to infer that a lock has been released.</p>
 * <p>The interface for the Locking System specified in this package does
 * not support implementations of the type used in Oracle.</p>
 * <p>Another strategy for locking is to use facilities provided by the
 * underlying operating system. For instance, most operating systems
 * support some form of file locking. Since database records are laid out into regions within a file
 * system, filesystem locks can be applied on records. No major database
 * system does this, however. Locking a region in the file would prevent
 * access to that region, which would cause other problems. Even when
 * systems do use filesytem locks, typically, some form of logical locking
 * is used. For example, in DBASE systems, a single byte would be locked in
 * the file to represent a record lock. In general, relying upon filesystem
 * locks can be source of numerous problems, such as portability of the system,
 * performance, etc.</p>
 */
package org.simpledbm.rss.api.locking;

