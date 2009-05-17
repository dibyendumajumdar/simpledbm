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

import org.simpledbm.rss.api.latch.Latch;
import org.simpledbm.rss.api.latch.LatchFactory;
import org.simpledbm.rss.api.platform.Platform;
import org.simpledbm.rss.api.platform.PlatformObjects;

/**
 * A factory for creating Latches.
 */
public final class LatchFactoryImpl implements LatchFactory {
	
	final Platform platform;
	
	final PlatformObjects po;

	public LatchFactoryImpl(Platform platform, Properties properties) {
		this.platform = platform;
		this.po = platform.getPlatformObjects(LatchFactory.LOGGER_NAME);
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
