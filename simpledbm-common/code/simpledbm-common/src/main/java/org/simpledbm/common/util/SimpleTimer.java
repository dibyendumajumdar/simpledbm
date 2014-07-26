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
package org.simpledbm.common.util;

import java.util.concurrent.locks.LockSupport;

/**
 * SimpleTimer enables bounded and unbounded waits. It uses the
 * {@link LockSupport#park()} and {@link LockSupport#parkNanos(long)} methods
 * for the waits. Both of these methods can return early, either due to an
 * outstanding unpark() signal, or spuriously. SimpleTimer allows the client to
 * check if the timer has expired. If the timer has not expired, the client can
 * invoke await() again, and wait for any remaining time.
 * <p>
 * Typical usage is as follows:
 * <p>
 * 
 * <pre>
 * SimpleTimer timer = new SimpleTimer(
 *   TimeUnit.NANOSECONDS.convert(
 *     10, TimeUnit.SECONDS)); // 10 seconds timer
 * while (some condition) {
 *   if (!timer.isExpired()) {
 *     timer.await();
 *   }
 *   else {
 *     break;
 *   }
 * }
 * </pre>
 * 
 * @author Dibyendu Majumdar
 */
public class SimpleTimer {

    /**
     * Time to wait in nanoseconds, -1 means wait unconditionally.
     */
    final long timeToWait;

    /**
     * Has this timer expired. Timer expires when time waited &gt; timeToWait.
     */
    boolean expired = false;

    /**
     * When the wait started; this is set when the timer object is created.
     */
    final long startTime;

    public SimpleTimer() {
        timeToWait = -1L;
        startTime = System.nanoTime();
    }

    public SimpleTimer(long nanos) {
        startTime = System.nanoTime();
        this.timeToWait = nanos < 0L ? -1L : nanos;
    }

    /**
     * Determines if the timer has expired. This is true if the time waited
     * exceeds time to wait.
     */
    public boolean isExpired() {
        if (expired)
            return true;
        if (timeToWait == -1) {
            return false;
        }
        long now = System.nanoTime();
        long timeWaited = now - startTime;
        if (timeWaited >= timeToWait) {
            expired = true;
        }
        return expired;
    }

    /**
     * If !isExpired(), wait for time remaining, or until the thread is
     * unparked. Time remaining is calculated as follows:
     * <p>
     * <ol>
     * <li>If timeToWait is -1, wait unconditionally.</li>
     * <li>Otherwise, timeToWait is calculated as the amount of time originally
     * requested minus the time already waited.</li>
     * </ol>
     */
    public void await() {
        if (expired) {
            return;
        }
        if (timeToWait == -1) {
            LockSupport.park();
            return;
        }
        long now = System.nanoTime();
        long timeWaited = now - startTime;
        if (timeWaited >= timeToWait) {
            expired = true;
            return;
        }
        long timeToWaitThisTime = timeToWait - timeWaited;
        assert timeToWaitThisTime > 0 && timeToWaitThisTime <= timeToWait;
        LockSupport.parkNanos(timeToWaitThisTime);
    }
}
