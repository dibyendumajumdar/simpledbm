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
package org.simpledbm.rss.impl.latch;

import java.util.Properties;

import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.util.mcat.MessageCatalog;
import org.simpledbm.rss.api.latch.Latch;
import org.simpledbm.rss.api.latch.LatchFactory;

/**
 * A factory for creating Latches.
 */
public final class LatchFactoryImpl implements LatchFactory {
	
	final Platform platform;
	
	final PlatformObjects po;

	public LatchFactoryImpl(Platform platform, Properties properties) {
		this.platform = platform;
		this.po = platform.getPlatformObjects(LatchFactory.LOGGER_NAME);
		
		MessageCatalog mcat = po.getMessageCatalog();
        // Latch Manager messages
        mcat.addMessage(
                "EH0001",
                "SIMPLEDBM-EH0001: Upgrade request {0} is invalid, as there is no prior lock");
        mcat.addMessage(
                "WH0002",
                "SIMPLEDBM-WH0002: Latch {0} is not compatible with requested mode {1}, timing out because this is a conditional request");
        mcat.addMessage(
                "EH0003",
                "SIMPLEDBM-EH0003: Invalid request because lock requested {0} is already being waited for by requester {1}");
        mcat.addMessage(
                "WH0004",
                "SIMPLEDBM-WH0004: Conversion request {0} is not compatible with granted group {1}, timing out because this is a conditional request");
        mcat.addMessage(
                "EH0005",
                "SIMPLEDBM-EH0005: Unexpected error while handling conversion request");
        mcat.addMessage("EH0006", "SIMPLEDBM-EH0006: Latch request {0} has timed out");
        mcat.addMessage(
                "EH0007",
                "SIMPLEDBM-EH0007: Invalid request as caller does not hold a lock on {0}");
        mcat.addMessage(
                "EH0008",
                "SIMPLEDBM-EH0008: Cannot release lock {0} as it is being waited for");
        mcat.addMessage(
                "EH0009",
                "SIMPLEDBM-EH0009: Invalid downgrade request: mode held {0}, mode to downgrade to {1}");

	}
	
    /* (non-Javadoc)
     * @see org.simpledbm.common.latch.LatchFactory#newReadWriteLatch()
     */
    public Latch newReadWriteLatch() {
        return new ReadWriteLatch();
    }

    /* (non-Javadoc)
     * @see org.simpledbm.common.latch.LatchFactory#newReadWriteUpdateLatch()
     */
    public Latch newReadWriteUpdateLatch() {
        return new NewReadWriteUpdateLatch(po);
    }

    /* (non-Javadoc)
     * @see org.simpledbm.common.latch.LatchFactory#newLatch()
     */
    public Latch newLatch() {
        return new NewReadWriteUpdateLatch(po);
    }

}
