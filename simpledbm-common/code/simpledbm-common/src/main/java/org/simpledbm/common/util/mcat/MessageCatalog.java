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
package org.simpledbm.common.util.mcat;

import java.text.MessageFormat;
import java.util.HashMap;

/**
 * Provides mechanism for obtaining localized messages.
 * 
 * @author Dibyendu Majumdar
 * @since 29 April 2007
 */
public class MessageCatalog {

    /*
     * This is an interim implementation - needs to be split into
     * interface/implementation at some stage. At present, we just want to
     * abstract the functionality from a client perspective.
     */

    HashMap<String, String> msgs;

    /*
     * messages have two codes:
     * 1st letter can be one of:
     * 	I - info
     *  W - warning
     *  E - error
     *  D - debug
     * 2nd letter indicates the module:
     *  L - logging
     *  U - util
     *  W - write ahead log
     *  T - tuple manager
     *  S - storage
     *  P - page manager
     *  R - registry
     *  X - transaction
     *  C - lock manager
     *  H - latch
     *  B - b-tree
     *  M - buffer manager
     *  F - free space manager
     *  O - slotted page manager
     *  V - server
     */

    void init() {
        msgs = new HashMap<String, String>();
        msgs
            .put(
                "WL0001",
                "SIMPLEDBM-WL0001: Failed to initialize JDK 1.4 logging system due to following error:");
        msgs
				.put("WL0002",
						"SIMPLEDBM-WL0002: Failed to initialize Log4J logging system");
        // Buffer Manager messages
        msgs
            .put(
                "EM0001",
                "SIMPLEDBM-EM0001: Error occurred while shutting down Buffer Manager");
        msgs
            .put(
                "EM0002",
                "SIMPLEDBM-EM0002: Error occurred while attempting to read page {0}");
        msgs
            .put(
                "EM0003",
                "SIMPLEDBM-EM0003: Error occurred while writing buffer pages, buffer writer failed causing buffer manager shutdown");
        msgs
            .put(
                "EM0004",
                "SIMPLEDBM-EM0004: Unexpected error - while attempting to read page {0} an empty frame could not be found: ");
        msgs
            .put(
                "EM0005",
                "SIMPLEDBM-EM0005: Unable to complete operation because Buffer Manager is shutting down");
        msgs
            .put(
                "EM0006",
                "SIMPLEDBM-EM0006: Unexpected error - while attempting to locate page {0} an empty frame could not be found or buffer manager is shutting down");
        msgs
            .put("EM0007", "SIMPLEDBM-EM0007: Latch mode in inconsistent state");
        msgs
            .put(
                "EM0008",
                "SIMPLEDBM-EM0008: Page can be marked dirty only if it has been latched exclusively");
        msgs
            .put(
                "EM0009",
                "SIMPLEDBM-EM0009: Upgrade of update latch requested but latch is not held in update mode currently");
        msgs
            .put(
                "EM0010",
                "SIMPLEDBM-EM0010: Downgrade of exclusive latch requested but latch is not held in exclusive mode currently");
        msgs.put("IM0011", "SIMPLEDBM-IM0011: Buffer Writer STARTED");
        msgs.put("IM0012", "SIMPLEDBM-IM0012: Buffer Writer STOPPED");

        // Free Space Manager messages
        msgs
            .put(
                "EF0001",
                "SIMPLEDBM-EF0001: Invalid number of bits [{0}] specified for space map page");
        msgs.put("EF0002", "SIMPLEDBM-EF0002: Container {0} does not exist");
        msgs
            .put(
                "EF0003",
                "SIMPLEDBM-EF0003: Unable to generate compensation for unknown log record type {0}");
        msgs
            .put(
                "EF0004",
                "SIMPLEDBM-EF0004: Unexpected error - page {0} does not belong to this map page {1}");
        msgs
            .put(
                "EF0005",
                "SIMPLEDBM-EF0005: Invalid state for Free Space Cursor - attempt to fix an SMP page when another page is already fixed");
        msgs
            .put(
                "EF0006",
                "SIMPLEDBM-EF0006: Invalid state for Free Space Cursor - attempt to access an SMP page that has not been fixed");

        // BTree Index Manager messages
        msgs
            .put(
                "EB0001",
                "SIMPLEDBM-EB0001: Unexpected error - missing child pointer in parent node");
        msgs
            .put(
                "EB0002",
                "SIMPLEDBM-EB0002: Unable to allocate a new page in the B-Tree container");
        msgs
            .put(
                "WB0003",
                "SIMPLEDBM-WB0003: Unique constraint would be violated by insertion of key={0} and location={1}");
        msgs.put(
            "EB0004",
            "SIMPLEDBM-EB0004: Unexpected error - key {0} not found");
        msgs
            .put(
                "EB0005",
                "SIMPLEDBM-EB0005: Unexpected error - current key k1={0} does not match expected key k2={1}");
        msgs
            .put(
                "EB0006",
                "SIMPLEDBM-EB0006: Unexpected error - search result returned null, B-Tree may be corrupt : search key = {0}");
        msgs
            .put(
                "EB0007",
                "SIMPLEDBM-EB0007: Unexpected error - while attempting to locate the split key in page {0}");
        msgs
            .put(
                "EB0008",
                "SIMPLEDBM-EB0008: Unexpected error - invalid binary search result while searching for {0}");
        msgs
            .put(
                "EB0009",
                "SIMPLEDBM-EB0009: Unexpected error - leaf page {0} encountered when expecting an index page");
        msgs.put(
            "EB0010",
            "SIMPLEDBM-EB0010: Index item {0} is not setup as leaf");
        msgs
            .put(
                "WB0011",
                "SIMPLEDBM-WB0011: fetchCompleted() has not been called after fetchNext()");
        msgs.put(
            "EB0012",
            "SIMPLEDBM-EB0012: Unexpected error - exception caught");
        msgs.put(
        	"EB0013",
        	"SIMPLEDBM-EB0013: Cannot insert or delete logical max key");

        // Class Utils messages
        msgs.put("EU0001", "SIMPLEDBM-EU0001: Unable to obtain classloader");
        msgs.put("EU0002", "SIMPLEDBM-EU0002: Unable to load resource {0}");

        // Server messages
        msgs.put("IV0001", "SIMPLEDBM-IV0001: SimpleDBM RSS Server STARTED");
        msgs.put("IV0002", "SIMPLEDBM-IV0002: SimpleDBM RSS Server STOPPED");
        msgs
            .put(
                "EV0003",
                "SIMPLEDBM-EV0003: SimpleDBM RSS Server cannot be started more than once");
        msgs.put(
            "EV0004",
            "SIMPLEDBM-EV0004: SimpleDBM RSS Server has not been started");
        msgs
            .put(
                "EV0005",
                "SIMPLEDBM-EV0005: Error starting SimpleDBM RSS Server, another instance may be running - error was: {0}");

        // Write Ahead Log Manager messages
        msgs
            .put(
                "EW0001",
                "SIMPLEDBM-EW0001: Log record is {0} bytes whereas maximum allowed log record size is {0}");
        msgs
            .put(
                "EW0002",
                "SIMPLEDBM-EW0002: Unexpected error ocurred while attempting to insert a log record");
        msgs
            .put(
                "EW0003",
                "SIMPLEDBM-EW0003: Log is already open or has encountered an error");
        msgs.put(
            "EW0004",
            "SIMPLEDBM-EW0004: Unexpected error occurred during shutdown");
        msgs.put("EW0005", "SIMPLEDBM-EW0005: Unexpected error occurred");
        msgs
            .put(
                "EW0006",
                "SIMPLEDBM-EW0006: Specified number of log control files {0} exceeds the maximum limit of {1}");
        msgs
            .put(
                "EW0007",
                "SIMPLEDBM-EW0007: Specified number of log groups {0} exceeds the maximum limit of {1}");
        msgs
            .put(
                "EW0008",
                "SIMPLEDBM-EW0008: Specified number of log files {0} exceeds the maximum limit of {1}");
        msgs
            .put(
                "EW0009",
                "SIMPLEDBM-EW0009: Error occurred while reading Log Anchor header information");
        msgs.put(
            "EW0010",
            "SIMPLEDBM-EW0010: Error occurred while reading Log Anchor body");
        msgs
            .put(
                "EW0011",
                "SIMPLEDBM-EW0011: Error occurred while validating Log Anchor - checksums do not match");
        msgs
            .put(
                "EW0012",
                "SIMPLEDBM-EW0012: Error occurred while reading header record for Log File {0}");
        msgs
            .put(
                "EW0013",
                "SIMPLEDBM-EW0013: Error occurred while opening Log File {0} - header is corrupted");
        msgs
            .put(
                "EW0014",
                "SIMPLEDBM-EW0014: Unexpected error occurred while closing Log File");
        msgs
            .put(
                "EW0015",
                "SIMPLEDBM-EW0015: Unexpected error occurred while closing Control File");
        msgs.put(
            "EW0016",
            "SIMPLEDBM-EW0016: Log file is not open or has encountered errors");
        msgs.put(
            "EW0017",
            "SIMPLEDBM-EW0017: Log file {0} has unexpected status {1}");
        msgs.put("EW0018", "SIMPLEDBM-EW0018: Unexpected error occurred");
        msgs
            .put(
                "EW0019",
                "SIMPLEDBM-EW0019: Error occurred while attempting to archive Log File");
        msgs
            .put(
                "EW0020",
                "SIMPLEDBM-EW0020: Error occurred while processing archive request - expected request {0} but got {1}");
        msgs
            .put(
                "EW0021",
                "SIMPLEDBM-EW0021: Error occurred while reading Log Record {0} - checksum mismatch");
        msgs
            .put(
                "EW0022",
                "SIMPLEDBM-EW0022: Error occurred while reading Log Record {0} - invalid log index");
        msgs
            .put(
                "EW0023",
                "SIMPLEDBM-EW0023: Error occurred while reading Log Record {0} - log header cannot be read");
        msgs
            .put(
                "EW0024",
                "SIMPLEDBM-EW0024: Log Record {0} has invalid length {1} - possibly garbage");
        msgs
            .put(
                "EW0025",
                "SIMPLEDBM-EW0025: Error occurred while reading Log Record {0} - read error");
        msgs.put(
            "EW0026",
            "SIMPLEDBM-EW0026: Error occurred while flushing the Log");
        msgs.put(
            "EW0027",
            "SIMPLEDBM-EW0027: Error occurred while archiving a Log File");
        msgs.put("IW0028", "SIMPLEDBM-IW0028: Log Writer STARTED");
        msgs.put("IW0029", "SIMPLEDBM-IW0029: Archive Cleaner STARTED");
        msgs.put("IW0030", "SIMPLEDBM-IW0030: Log Writer STOPPED");
        msgs.put("IW0031", "SIMPLEDBM-IW0031: Archive Cleaner STOPPED");
        msgs.put("IW0032", "SIMPLEDBM-IW0032: Write Ahead Log Manager STOPPED");

        // Latch Manager messages
        msgs
            .put(
                "EH0001",
                "SIMPLEDBM-EH0001: Upgrade request {0} is invalid, as there is no prior lock");
        msgs
            .put(
                "WH0002",
                "SIMPLEDBM-WH0002: Latch {0} is not compatible with requested mode {1}, timing out because this is a conditional request");
        msgs
            .put(
                "EH0003",
                "SIMPLEDBM-EH0003: Invalid request because lock requested {0} is already being waited for by requester {1}");
        msgs
            .put(
                "WH0004",
                "SIMPLEDBM-WH0004: Conversion request {0} is not compatible with granted group {1}, timing out because this is a conditional request");
        msgs
            .put(
                "EH0005",
                "SIMPLEDBM-EH0005: Unexpected error while handling conversion request");
        msgs.put("EH0006", "SIMPLEDBM-EH0006: Latch request {0} has timed out");
        msgs
            .put(
                "EH0007",
                "SIMPLEDBM-EH0007: Invalid request as caller does not hold a lock on {0}");
        msgs
            .put(
                "EH0008",
                "SIMPLEDBM-EH0008: Cannot release lock {0} as it is being waited for");
        msgs
            .put(
                "EH0009",
                "SIMPLEDBM-EH0009: Invalid downgrade request: mode held {0}, mode to downgrade to {1}");

        // Lock Manager messages
        msgs.put("EC0001", "SIMPLEDBM-EC0001: Lock request {0} timed out");
        msgs.put(
            "WC0002",
            "SIMPLEDBM-WC0002: Lock request {0} failed due to a deadlock");
        msgs
            .put(
                "EC0099",
                "SIMPLEDBM-EC0099: Unexpected error occurred while attempting to acquire lock request {0}");
        msgs
            .put(
                "EC0003",
                "SIMPLEDBM-EC0003: Invalid request because lock requested {0} is already being waited for by requester {1}");
        msgs
            .put(
                "WC0004",
                "SIMPLEDBM-WC0004: Lock request {0} is not compatible with granted group {1}, timing out because this is a conditional request");
        msgs
            .put(
                "EC0005",
                "SIMPLEDBM-EC0005: Unexpected error while handling conversion request");
        msgs
            .put(
                "EC0008",
                "SIMPLEDBM-EC0008: Invalid request to release lock {0} as it is being waited for");
        msgs
            .put(
                "EC0009",
                "SIMPLEDBM-EC0009: Invalid downgrade request: mode held {0}, mode to downgrade to {1}");
        msgs
            .put(
                "EC0010",
                "SIMPLEDBM-EC0010: Listener {0} failed unexpectedly: lock parameters {1}");
//        msgs
//            .put(
//                "WC0011",
//                "SIMPLEDBM-WC0011: Detected deadlock cycle: \nR1 {0} \n\t(victim) waiting for \n\tR2 {1}\n\tlock items:\n\tR1 {2}\n\tR2 {3}");
        msgs
        	.put(
            "WC0011",
            "SIMPLEDBM-WC0011: Detected deadlock cycle: {2} {3}");
        msgs.put("IC0012", "SIMPLEDBM-IC0012: Deadlock detector STARTED");
        msgs.put("IC0013", "SIMPLEDBM-IC0013: Deadlock detector STOPPED");
        msgs.put("IC0014", "SIMPLEDBM-IC0014: Dumping lock table");
        msgs.put("IC0015", "SIMPLEDBM-IC0015: LockItem = {0}");

        // Page Manager messages
        msgs
            .put(
                "EP0001",
                "SIMPLEDBM-EP0001: Error occurred while reading page {0}: the number of bytes read is {1}; but expected {2} bytes");
        msgs
            .put(
                "EP0002",
                "SIMPLEDBM-EP0002: Error occurred while reading page {0}: container not available");
        msgs
            .put(
                "EP0003",
                "SIMPLEDBM-EP0003: Error occurred while writing page {0}: container not available");
        msgs
        	.put(
        	    "EP0004",
                "SIMPLEDBM-EP0004: Error occurred while reading page {0}: checksum invalid");
        msgs.put("EP0005", "SIMPLEDBM-EP0005: A PageFactory was not available to handle page type {0}");

        // Object Registry messages
        msgs.put(
            "WR0001",
            "SIMPLEDBM-WR0001: Duplicate registration of type {0} ignored");
        msgs
            .put(
                "ER0002",
                "SIMPLEDBM-ER0002: Duplicate registration of type {0} does not match previous registration: previous type {1}, new type {2}");
        msgs
            .put(
                "WR0003",
                "SIMPLEDBM-WR0003: Duplicate registration of singleton {0} ignored");
        msgs
            .put(
                "ER0004",
                "SIMPLEDBM-ER0004: Duplicate registration of singleton {0} does not match previous registration: previous object {1}, new object {2}");
        msgs
            .put(
                "ER0005",
                "SIMPLEDBM-ER0005: Error occurred when attempting to load class {0}");
        msgs.put("ER0006", "SIMPLEDBM-ER0006: Unknown typecode {0}");
        msgs
            .put(
                "ER0007",
                "SIMPLEDBM-ER0007: Error occurred when attempting to create new instance of type {0} class {1}");      
        
        // Slotted Page Manager
        msgs
            .put(
                "EO0001",
                "SIMPLEDBM-EO0001: Cannot insert item {0} into page {1} due to lack of space");
        msgs
            .put(
                "EO0002",
                "SIMPLEDBM-EO0002: Cannot insert item {0} of size {3} into page at slot {2} due to lack of space: page contents = {1}");
        msgs
            .put(
                "EO0003",
                "SIMPLEDBM-EO0003: Cannot access item at slot {0}, number of slots in page is {1}");
        msgs
            .put(
                "EO0004",
                "SIMPLEDBM-EO0004: Cannot modify page contents as page is not latched EXCLUSIVELY");
        msgs.put("EO0005", "SIMPLEDBM-EO0005: Invalid page contents");
        msgs
            .put(
                "EO0006",
                "SIMPLEDBM-EO0006: Unexpected error: failed to find insertion point in page");

        // storage manager messages
        msgs.put(
            "ES0001",
            "SIMPLEDBM-ES0001: StorageContainer {0} is not valid");
        msgs
            .put(
                "ES0003",
                "SIMPLEDBM-ES0003: Error occurred while writing to StorageContainer {0}");
        msgs
            .put(
                "ES0004",
                "SIMPLEDBM-ES0004: Error occurred while reading from StorageContainer {0}");
        msgs
            .put(
                "ES0005",
                "SIMPLEDBM-ES0005: Error occurred while flushing StorageContainer {0}");
        msgs
            .put(
                "ES0006",
                "SIMPLEDBM-ES0006: Error occurred while closing StorageContainer {0}");
        msgs.put(
            "ES0007",
            "SIMPLEDBM-ES0007: StorageContainer {0} is already locked");
        msgs
            .put(
                "ES0008",
                "SIMPLEDBM-ES0008: An exclusive lock could not be obtained on StorageContainer {0}");
        msgs.put(
            "ES0009",
            "SIMPLEDBM-ES0009: StorageContainer {0} is not locked");
        msgs
            .put(
                "ES0010",
                "SIMPLEDBM-ES0010: Error occurred while releasing lock on StorageContainer {0}");
        msgs.put(
            "ES0011",
            "SIMPLEDBM-ES0011: Directory specified by {0}={1} does not exist");
        msgs.put(
            "ES0012",
            "SIMPLEDBM-ES0012: Error creating directory specified by {0}={1}");
        msgs
            .put(
                "ES0013",
                "SIMPLEDBM-ES0013: Specified base path {0}={1} is not a directory or is not accessible");
        msgs.put(
            "ES0014",
            "SIMPLEDBM-ES0014: Path name {0} must be a directory");
        msgs.put("ES0015", "SIMPLEDBM-ES0015: Error creating directory {0}");
        msgs.put(
            "ES0016",
            "SIMPLEDBM-ES0016: Unable to delete StorageContainer {0}");
        msgs
            .put(
                "ES0017",
                "SIMPLEDBM-ES0017: Unable to create StorageContainer {0} because an object of the name already exists");
        msgs
            .put(
                "ES0018",
                "SIMPLEDBM-ES0018: Unexpected error occurred while creating StorageContainer {0}");
        msgs
            .put(
                "ES0019",
                "SIMPLEDBM-ES0019: StorageContainer {0} does not exist or is not accessible");
        msgs
            .put(
                "ES0020",
                "SIMPLEDBM-ES0020: Unexpected error occurred while opening StorageContainer {0}");
        msgs
            .put(
                "ES0021",
                "SIMPLEDBM-ES0021: Unable to delete {0} as named object is not a StorageContainer");
        msgs.put("IS0022", "SIMPLEDBM-IS0022: StorageManager STOPPED");
        msgs
            .put(
                "ES0023",
                "SIMPLEDBM-ES0023: Unexpected error occurred while closing StorageContainer {0}");
        msgs.put(
                "ES0024",
                "SIMPLEDBM-ES0024: Unable to delete path name {0}");

        // tuple manager messages
        msgs.put(
            "ET0001",
            "SIMPLEDBM-ET0001: This operation has not yet been implemented");
        msgs
            .put(
                "ET0002",
                "SIMPLEDBM-ET0002: Comparison is not possible because the supplied argument {0} is not of type {1}");
        msgs.put(
            "ET0003",
            "SIMPLEDBM-ET0003: TupleId {0} has not been initialized");
        msgs
            .put(
                "ET0004",
                "SIMPLEDBM-ET0004: Failed to allocate space for TupleContainer {0}");
        msgs.put("ET0005", "SIMPLEDBM-ET0005: Invalid location {0}");
        msgs
            .put(
                "ET0006",
                "SIMPLEDBM-ET0006: Unexpected IO error occurred while reading tuple data");
        msgs
            .put(
                "ET0007",
                "SIMPLEDBM-ET0007: Invalid call to completeInsert() as startInsert() did not succeed");

        // transaction manager messages
        msgs.put("EX0001", "SIMPLEDBM-EX0001: Unknown transaction module {0}");
        msgs
            .put(
                "EX0002",
                "SIMPLEDBM-EX0002: Invalid log operation {0}: A Redoable log record must not implement the NonTransactionRelatedOperation interface");
        msgs
            .put(
                "EX0003",
                "SIMPLEDBM-EX0003: Invalid log operation {0}: A Redoable log record must not implement the PostCommitAction interface");
        msgs
            .put(
                "EX0004",
                "SIMPLEDBM-EX0004: Invalid log operation {0}: A Redoable log record must not implement the Checkpoint interface");
        msgs
            .put(
                "EX0005",
                "SIMPLEDBM-EX0005: Invalid log operation {0}: A Redoable log record must not implement the TrxControl interface");
        msgs
            .put(
                "EX0006",
                "SIMPLEDBM-EX0006: Invalid log operation {0}: An Undoable record must not implement MultiPageRedo interface");
        msgs
            .put(
                "EX0007",
                "SIMPLEDBM-EX0007: Invalid log operation {0}: An Undoable record must not implement Compensation interface");
        msgs
            .put(
                "EX0008",
                "SIMPLEDBM-EX0008: Invalid log operation {0}: An Undoable record must not implement ContainerDelete interface");
        msgs.put(
            "EX0009",
            "SIMPLEDBM-EX0009: Log operation {0} is not an instance of {1}");
        msgs
            .put(
                "EX0010",
                "SIMPLEDBM-EX0010: The StorageContainer registered as {0} is named {1} instead of {2}");
        msgs
            .put(
                "EX0011",
                "SIMPLEDBM-EX0011: Unexpected EOF occurred in write ahead log while reading checkpoint records");
        msgs.put(
            "WX0012",
            "SIMPLEDBM-WX0012: Ignoring exception caused due to {0}");
        msgs.put("IX0013", "SIMPLEDBM-IX0013: Checkpoint Writer STARTED");
        msgs
            .put(
                "EX0014",
                "SIMPLEDBM-EX0014: Error occurred while shutting down Transaction Manager");
        msgs.put("IX0015", "SIMPLEDBM-IX0015: Checkpoint Writer STOPPED");
        msgs.put("IX0015", "SIMPLEDBM-IX0015: Checkpoint Writer STOPPED");
        msgs
            .put(
                "EX0016",
                "SIMPLEDBM-EX0016: Invalid state: Nested top action cannot be started as one is already active");
        msgs
            .put(
                "EX0017",
                "SIMPLEDBM-EX0017: Invalid state: Nested top action cannot be completed as there is none active");
        msgs.put(
            "EX0018",
            "SIMPLEDBM-EX0018: Error occurred while writing a Checkpoint");
        msgs.put("IX0019", "Property {0} set to {1}");

    }

    private MessageCatalog() {
    	init();
    }
    
    /**
     * Add a key/message to the message catalog.
     * @param key A key to identify the message
     * @param message Message string
     */
    public void addMessage(String key, String message) {
    	msgs.put(key, message);
    }
    
    public String getMessage(String key) {
        String s = msgs.get(key);
        if (s != null) {
            return s;
        }
        return "SIMPLEDBM-U9999: Unknown message key - " + key;
    }

    public String getMessage(String key, Object... args) {
        String s = msgs.get(key);
        if (s != null) {
            return MessageFormat.format(s, args);
        }
        return "SIMPLEDBM-U9999: Unknown message key - " + key;
    }
    
    public static synchronized MessageCatalog getMessageCatalog() {
   		return new MessageCatalog();
    }
}
