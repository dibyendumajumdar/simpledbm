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
package org.simpledbm.common.api.thread;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * The Scheduler allows background tasks to be executed. It is possible to
 * submit tasks for immediate or delayed execution. it is also possible to
 * request that a task be rerun at regular intervals.
 * 
 * @author dibyendumajumdar
 */
public interface Scheduler {

    enum Priority {
        NORMAL, SERVER_TASK
    }

    /**
     * Executes the given command at some time in the future.
     * 
     * @param command the runnable task
     * @throws RejectedExecutionException if this task cannot be accepted for
     *             execution.
     * @throws NullPointerException if command is null
     */
    void execute(Priority priority, Runnable command);

    /**
     * Creates and executes a periodic action that becomes enabled first after
     * the given initial delay, and subsequently with the given delay between
     * the termination of one execution and the commencement of the next. If any
     * execution of the task encounters an exception, subsequent executions are
     * suppressed. Otherwise, the task will only terminate via termination of
     * the executor.
     * 
     * @param command the task to execute
     * @param initialDelay the time to delay first execution
     * @param delay the delay between the termination of one execution and the
     *            commencement of the next
     * @param unit the time unit of the initialDelay and delay parameters
     * @throws RejectedExecutionException if the task cannot be scheduled for
     *             execution
     * @throws NullPointerException if command is null
     * @throws IllegalArgumentException if delay less than or equal to zero
     */
    public ScheduledFuture<?> scheduleWithFixedDelay(Priority priority,
            Runnable command, long initialDelay, long delay, TimeUnit unit);

    /**
     * Initiates an orderly shutdown in which previously submitted tasks are
     * executed, but no new tasks will be accepted. Invocation has no additional
     * effect if already shut down.
     * 
     * @throws SecurityException if a security manager exists and shutting down
     *             may manipulate threads that the caller is not permitted to
     *             modify because it does not hold
     *             {@link java.lang.RuntimePermission}<tt>("modifyThread")</tt>,
     *             or the security manager's <tt>checkAccess</tt> method denies
     *             access.
     */
    void shutdown();

}
