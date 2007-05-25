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
 *    Email  : dibyendu@mazumdar.demon.co.uk
 */
package org.simpledbm.rss.util.mcat;

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
	
	static HashMap<String,String> msgs;
	
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
	
	static {
		msgs = new HashMap<String,String>();
		msgs.put("WL0001", "SIMPLEDBM-WL0001: Failed to initialize logging system due to following error:");
		msgs.put("EM0001", "SIMPLEDBM-EM0001: Error occurred while shutting down Buffer Manager");
		msgs.put("EM0002", "SIMPLEDBM-EM0002: Error occurred while attempting to read page:");
		msgs.put("EM0003", "SIMPLEDBM-EM0003: Error occurred while writing buffer pages, buffer writer failed causing buffer manager shutdown");
		msgs.put("EM0004", "SIMPLEDBM-EM0004: Unexpected error - while attempting to read a page an empty frame could not be found: ");
		msgs.put("EM0005", "SIMPLEDBM-EM0005: Unable to complete operation because Buffer Manager is shutting down");
		msgs.put("EM0006", "SIMPLEDBM-EM0006: Unexpected error - while attempting to locate a page an empty frame could not be found or buffer manager is shutting down: ");
		msgs.put("EM0007", "SIMPLEDBM-EM0007: Latch mode in inconsistent state");
		msgs.put("EM0008", "SIMPLEDBM-EM0008: Page can be marked dirty only if it has been latched exclusively");
		msgs.put("EM0009", "SIMPLEDBM-EM0009: Upgrade of update latch requested but latch is not held in update mode currently");
		msgs.put("EM0010", "SIMPLEDBM-EM0010: Downgrade of exclusive latch requested but latch is not held in exclusive mode currently");
		msgs.put("EF0001", "SIMPLEDBM-EF0001: Invalid number of bits specified for space map page: ");
		msgs.put("EF0002", "SIMPLEDBM-EF0002: Specified container does not exist: ");
		msgs.put("EF0003", "SIMPLEDBM-EF0003: Unable to generate compensation for unknown log record type: ");
		msgs.put("EF0004", "SIMPLEDBM-EF0004: Unexpected error - specified page does not belong to this space map page: ");
		msgs.put("EF0005", "SIMPLEDBM-EF0005: Invalid state for Free Space Cursor - attempt to fix an SMP page when another page is already fixed");
		msgs.put("EF0006", "SIMPLEDBM-EF0006: Invalid state for Free Space Cursor - attempt to access an SMP page that has not been fixed");
		msgs.put("EB0001", "SIMPLEDBM-EB0001: Unexpected error - missing child pointer in parent node");
		msgs.put("EB0002", "SIMPLEDBM-EB0002: Unable to allocate a new page in the B-Tree container");
		msgs.put("WB0003", "SIMPLEDBM-WB0003: Unique constraint would be violated by insertion of: ");
		msgs.put("EB0004", "SIMPLEDBM-EB0004: Unexpected error - key to be deleted not found: ");
		msgs.put("EB0005", "SIMPLEDBM-EB0005: Unexpected error - current key k1 does not match expected key k2: ");
		msgs.put("EB0006", "SIMPLEDBM-EB0006: Unexpected error - search result returned null, B-Tree may be corrupt : search key = ");
		msgs.put("EB0007", "SIMPLEDBM-EB0007: Unexpected error - while attempting to locate the split key in a page");
		msgs.put("EB0008", "SIMPLEDBM-EB0008: Unexpected error - invalid binary search result while searching for ");
		msgs.put("EB0009", "SIMPLEDBM-EB0009: Unexpected error - leaf page encountered when expecting an index page");
		msgs.put("EB0010", "SIMPLEDBM-EB0010: Supplied index item not setup as leaf: ");
		msgs.put("WB0011", "SIMPLEDBM-WB0011: fetchCompleted() has not been called after fetchNext()");
		msgs.put("EB0012", "SIMPLEDBM-EB0012: Unexpected error - exception caught");
		msgs.put("EU0001", "SIMPLEDBM-EU0001: Unable to obtain classloader");
		msgs.put("EU0002", "SIMPLEDBM-EU0002: Unable to load resource {0}");
		msgs.put("IV0001", "SIMPLEDBM-IV0001: SimpleDBM RSS Server startup completed");
		msgs.put("IV0002", "SIMPLEDBM-IV0002: SimpleDBM RSS Server shutdown completed");
		msgs.put("EV0003", "SIMPLEDBM-EV0003: SimpleDBM RSS Server cannot be started more than once");
		msgs.put("EV0004", "SIMPLEDBM-EV0004: SimpleDBM RSS Server has not been started");
		msgs.put("EV0005", "SIMPLEDBM-EV0005: Error starting SimpleDBM RSS Server, another instance may be running - error was: {0}");
		msgs.put("EW0001", "SIMPLEDBM-EW0001: Log record is {0} bytes whereas maximum allowed log record size is {0}");
		msgs.put("EW0002", "SIMPLEDBM-EW0002: Unexpected error ocurred while attempting to insert a log record");
		msgs.put("EW0003", "SIMPLEDBM-EW0003: Log is already open or has encountered an error");
		msgs.put("EW0004", "SIMPLEDBM-EW0004: Unexpected error occurred during shutdown");
		msgs.put("EW0005", "SIMPLEDBM-EW0005: Unexpected error occurred");
		msgs.put("EW0006", "SIMPLEDBM-EW0006: Specified number of log control files {0} exceeds the maximum limit of {1}");
		msgs.put("EW0007", "SIMPLEDBM-EW0007: Specified number of log groups {0} exceeds the maximum limit of {1}");
		msgs.put("EW0008", "SIMPLEDBM-EW0008: Specified number of log files {0} exceeds the maximum limit of {1}");
		msgs.put("EW0009", "SIMPLEDBM-EW0009: Error occurred while reading Log Anchor header information");
		msgs.put("EW0010", "SIMPLEDBM-EW0010: Error occurred while reading Log Anchor body");
		msgs.put("EW0011", "SIMPLEDBM-EW0011: Error occurred while validating Log Anchor - checksums do not match");
		msgs.put("EW0012", "SIMPLEDBM-EW0012: Error occurred while reading header record for Log File {0}");
		msgs.put("EW0013", "SIMPLEDBM-EW0013: Error occurred while opening Log File {0} - header is corrupted");
		msgs.put("EW0014", "SIMPLEDBM-EW0014: Unexpected error occurred while closing Log File");
		msgs.put("EW0015", "SIMPLEDBM-EW0015: Unexpected error occurred while closing Control File");
		msgs.put("EW0016", "SIMPLEDBM-EW0016: Log file is not open or has encountered errors");
		msgs.put("EW0017", "SIMPLEDBM-EW0017: Log file {0} has unexpected status {1}");
		msgs.put("EW0018", "SIMPLEDBM-EW0018: Unexpected error occurred");
		msgs.put("EW0019", "SIMPLEDBM-EW0019: Error occurred while attempting to archive Log File");
		msgs.put("EW0020", "SIMPLEDBM-EW0020: Error occurred while processing archive request - expected request {0} but got {1}");
		msgs.put("EW0021", "SIMPLEDBM-EW0021: Error occurred while reading Log Record {0} - checksum mismatch");
		msgs.put("EW0022", "SIMPLEDBM-EW0022: Error occurred while reading Log Record {0} - invalid log index");
		msgs.put("EW0023", "SIMPLEDBM-EW0023: Error occurred while reading Log Record {0} - log header cannot be read");
		msgs.put("EW0024", "SIMPLEDBM-EW0024: Log Record {0} has invalid length {1} - possibly garbage");
		msgs.put("EW0025", "SIMPLEDBM-EW0025: Error occurred while reading Log Record {0} - read error");
		msgs.put("EW0026", "SIMPLEDBM-EW0026: Error occurred while flushing the Log");
		msgs.put("EW0027", "SIMPLEDBM-EW0027: Error occurred while archiving a Log File");
		msgs.put("EH0001", "SIMPLEDBM-EH0001: Invalid upgrade request, as there is no prior lock: {0}");
		msgs.put("WH0002", "SIMPLEDBM-WH0002: Latch {0} is not compatible with requested mode {1}, timing out because this is a conditional request");
		msgs.put("EH0003", "SIMPLEDBM-EH0003: Invalid request because lock requested {0} is already being waited for by requester {1}");
		msgs.put("EH0004", "SIMPLEDBM-EH0004: Conversion request {0} is not compatible with granted group {1}, timing out because this is a conditional request");
		msgs.put("EH0005", "SIMPLEDBM-EH0005: Unexpected error while handling conversion request");
		msgs.put("EH0006", "SIMPLEDBM-EH0006: Latch request {0} has timed out");
		msgs.put("EH0007", "SIMPLEDBM-EH0007: Invalid request as caller does not hold a lock on {0}");
		msgs.put("EH0008", "SIMPLEDBM-EH0008: Cannot release lock {0} as it is being waited for");
		msgs.put("EH0009", "SIMPLEDBM-EH0009: Invalid downgrade request: mode held {0}, mode to downgrade to {1}");
		msgs.put("EC0001", "SIMPLEDBM-EC0001: Lock request {0} timed out");
		msgs.put("WC0002", "SIMPLEDBM-WC0002: Lock request {0} failed due to a deadlock");
		msgs.put("EC0003", "SIMPLEDBM-EC0003: Unexpected error occurred while attempting to acquire lock request {0}");
		
	}
	
	public String getMessage(String key) {
		String s = msgs.get(key);
		if (s != null) {
			return s;
		}		
		return "SIMPLEDBM-U9999: Unknown message key - " + key;
	}
	
	public String getMessage(String key, Object ...args) {
		String s = msgs.get(key);
		if (s != null) {
			return MessageFormat.format(s, args);
		}
		return "SIMPLEDBM-U9999: Unknown message key - " + key;
	}
}
