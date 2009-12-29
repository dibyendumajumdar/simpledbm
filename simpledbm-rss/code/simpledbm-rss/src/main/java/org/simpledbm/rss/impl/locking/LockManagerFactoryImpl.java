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
package org.simpledbm.rss.impl.locking;

import java.util.Properties;

import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.util.mcat.MessageCatalog;
import org.simpledbm.rss.api.latch.LatchFactory;
import org.simpledbm.rss.api.locking.LockManager;
import org.simpledbm.rss.api.locking.LockMgrFactory;

/**
 * Default implementation of a LockMgrFactory.
 * 
 * @author Dibyendu Majumdar
 */
public final class LockManagerFactoryImpl implements LockMgrFactory {

	final Platform platform;
	final PlatformObjects po;
	
	public LockManagerFactoryImpl(Platform platform, Properties props) {
		this.platform = platform;
		this.po = platform.getPlatformObjects(LockManager.LOGGER_NAME);
		MessageCatalog mcat = po.getMessageCatalog();
		
        // Lock Manager messages
        mcat.addMessage("EC0001", "SIMPLEDBM-EC0001: Lock request {0} timed out");
        mcat.addMessage(
            "WC0002",
            "SIMPLEDBM-WC0002: Lock request {0} failed due to a deadlock");
        mcat.addMessage(
                "EC0099",
                "SIMPLEDBM-EC0099: Unexpected error occurred while attempting to acquire lock request {0}");
        mcat.addMessage(
                "EC0003",
                "SIMPLEDBM-EC0003: Invalid request because lock requested {0} is already being waited for by requester {1}");
        mcat.addMessage(
                "WC0004",
                "SIMPLEDBM-WC0004: Lock request {0} is not compatible with granted group {1}, timing out because this is a conditional request");
        mcat.addMessage(
                "EC0005",
                "SIMPLEDBM-EC0005: Unexpected error while handling conversion request");
        mcat.addMessage(
                "EC0008",
                "SIMPLEDBM-EC0008: Invalid request to release lock {0} as it is being waited for");
        mcat.addMessage(
                "EC0009",
                "SIMPLEDBM-EC0009: Invalid downgrade request: mode held {0}, mode to downgrade to {1}");
        mcat.addMessage(
                "EC0010",
                "SIMPLEDBM-EC0010: Listener {0} failed unexpectedly: lock parameters {1}");
//        msgs
//            .put(
//                "WC0011",
//                "SIMPLEDBM-WC0011: Detected deadlock cycle: \nR1 {0} \n\t(victim) waiting for \n\tR2 {1}\n\tlock items:\n\tR1 {2}\n\tR2 {3}");
        mcat.addMessage(
            "WC0011",
            "SIMPLEDBM-WC0011: Detected deadlock cycle: {2} {3}");
        mcat.addMessage("IC0012", "SIMPLEDBM-IC0012: Deadlock detector STARTED");
        mcat.addMessage("IC0013", "SIMPLEDBM-IC0013: Deadlock detector STOPPED");
        mcat.addMessage("IC0014", "SIMPLEDBM-IC0014: Dumping lock table");
        mcat.addMessage("IC0015", "SIMPLEDBM-IC0015: LockItem = {0}");
		
	}
	
    /**
     * Creates a new LockMgr object.
     */
    public final LockManager create(LatchFactory latchFactory, Properties props) {
        int deadlockInterval = 15;
        if (props != null) {
            String s = props.getProperty(
                "lock.deadlock.detection.interval",
                "15");
            deadlockInterval = Integer.parseInt(s);
        }
        LockManager lockmgr = new LockManagerImpl(po, latchFactory, props);
        lockmgr.setDeadlockDetectorInterval(deadlockInterval);
        return lockmgr;
    }

}
