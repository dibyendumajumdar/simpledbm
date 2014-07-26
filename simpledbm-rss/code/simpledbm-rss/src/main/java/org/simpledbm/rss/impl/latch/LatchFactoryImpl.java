/**
 * DO NOT REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Contributor(s):
 *
 * The Original Software is SimpleDBM (www.simpledbm.org).
 * The Initial Developer of the Original Software is Dibyendu Majumdar.
 *
 * Portions Copyright 2005-2014 Dibyendu Majumdar. All Rights Reserved.
 *
 * The contents of this file are subject to the terms of the
 * Apache License Version 2 (the "APL"). You may not use this
 * file except in compliance with the License. A copy of the
 * APL may be obtained from:
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the APL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the APL, the GPL or the LGPL.
 *
 * Copies of GPL and LGPL may be obtained from:
 * http://www.gnu.org/licenses/license-list.html
 */
package org.simpledbm.rss.impl.latch;

import java.util.Properties;

import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.util.mcat.Message;
import org.simpledbm.common.util.mcat.MessageType;
import org.simpledbm.rss.api.latch.Latch;
import org.simpledbm.rss.api.latch.LatchFactory;

/**
 * A factory for creating Latches.
 */
public final class LatchFactoryImpl implements LatchFactory {

    final Platform platform;

    final PlatformObjects po;

    // Latch Manager messages
    static Message m_EH0001 = new Message('R', 'H', MessageType.ERROR, 1,
            "Upgrade request {0} is invalid, as there is no prior lock");
    static Message m_WH0002 = new Message(
            'R',
            'H',
            MessageType.WARN,
            2,
            "Latch {0} is not compatible with requested mode {1}, timing out because this is a conditional request");
    static Message m_EH0003 = new Message(
            'R',
            'H',
            MessageType.ERROR,
            3,
            "Invalid request because lock requested {0} is already being waited for by requester {1}");
    static Message m_WH0004 = new Message(
            'R',
            'H',
            MessageType.WARN,
            4,
            "Conversion request {0} is not compatible with granted group {1}, timing out because this is a conditional request");
    static Message m_EH0005 = new Message('R', 'H', MessageType.ERROR, 5,
            "Unexpected error while handling conversion request");
    static Message m_EH0006 = new Message('R', 'H', MessageType.ERROR, 6,
            "Latch request {0} has timed out");
    static Message m_EH0007 = new Message('R', 'H', MessageType.ERROR, 7,
            "Invalid request as caller does not hold a lock on {0}");
    static Message m_EH0008 = new Message('R', 'H', MessageType.ERROR, 8,
            "Cannot release lock {0} as it is being waited for");
    static Message m_EH0009 = new Message('R', 'H', MessageType.ERROR, 9,
            "Invalid downgrade request: mode held {0}, mode to downgrade to {1}");

    public LatchFactoryImpl(Platform platform, Properties properties) {
        this.platform = platform;
        this.po = platform.getPlatformObjects(LatchFactory.LOGGER_NAME);
    }

    /* (non-Javadoc)
     * @see org.simpledbm.common.latch.LatchFactory#newReadWriteLatch()
     */
    public Latch newReadWriteLatch() {
        return new ReadWriteLatch(po);
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
