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
	 *  B - btree
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
		msgs.put("EM0004", "SIMPLEDBM-EM0004: Unexpected error: while attempting to read a page an empty frame could not be found: ");
		msgs.put("EM0005", "SIMPLEDBM-EM0005: Unable to complete operation because Buffer Manager is shutting down");
		msgs.put("EM0006", "SIMPLEDBM-EM0006: Unexpected error: while attempting to locate a page an empty frame could not be found or buffer manager is shutting down: ");
		msgs.put("EM0007", "SIMPLEDBM-EM0007: Latch mode in inconsistent state");
		msgs.put("EM0008", "SIMPLEDBM-EM0008: Page can be marked dirty only if it has been latched exclusively");
		msgs.put("EM0009", "SIMPLEDBM-EM0009: Upgrade of update latch requested but latch is not held in update mode currently");
		msgs.put("EM0010", "SIMPLEDBM-EM0010: Downgrade of exclusive latch requested but latch is not held in exclusive mode currently");
	}
	
	public String getMessage(String key) {
		
		String s = msgs.get(key);
		if (s != null) {
			return s;
		}
		
		return "U9999: Unknown message";
	}
	
}
