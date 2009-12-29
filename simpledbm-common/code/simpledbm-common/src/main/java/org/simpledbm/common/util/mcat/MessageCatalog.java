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

        // Class Utils messages
        msgs.put("EU0001", "SIMPLEDBM-EU0001: Unable to obtain classloader");
        msgs.put("EU0002", "SIMPLEDBM-EU0002: Unable to load resource {0}");
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
