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
package org.simpledbm.common.tools.diagnostics;

import org.simpledbm.common.util.WrappingSequencer;

/**
 * An efficient thread safe but lock free mechanism to generate trace messages.
 * Uses a ring buffer. Messages are stored in memory so that there is very
 * little performance impact. Each message is tagged with the thread id, and a
 * sequence number. The sequence number wraps around when it reaches 2147483646.
 * <p>
 * The design of TraceBuffer mechanism was inspired by the article in <cite>DDJ
 * April 23, 2007, Multi-threaded Debugging Techniques by Shameem Akhter and
 * Jason Roberts</cite>. This article is an excerpt from the book
 * <cite>Multi-Core Programming</cite> by the same authors.
 * 
 * @author dibyendu majumdar
 * @since 26 July 2008
 */
public final class TraceBuffer {

    public interface TraceVisitor {
        void visit(long tid, int seq, int msg, int d1, int d2, int d3, int d4);
    }

    /**
     * A atomic sequence number.
     */
    final WrappingSequencer seq = new WrappingSequencer(Integer.MAX_VALUE);

    /**
     * Default size of the trace buffer.
     */
    final int SIZE = 5000;

    /**
     * The trace buffer array where events are stored.
     */
    final TraceElement[] traceBuffer;

    /**
     * Enables/disables the trace functionality
     */
    final boolean enabled;

    /*
     * For performance reasons, we only allow numeric arguments to be supplied
     * as trace message identifiers and arguments. This avoids expensive object
     * allocations. The trace message is inserted at the next position in the
     * traceBuffer array, the next pointer wraps to the beginning of the array
     * when it reaches the end.
     */

    public TraceBuffer(boolean enabled) {
        this.enabled = enabled;
        traceBuffer = new TraceElement[SIZE];
        for (int i = 0; i < SIZE; i++) {
            traceBuffer[i] = new TraceElement();
        }
    }

    public void event(int msg) {
        if (!enabled) 
            return;
        int next = seq.getNext();
        int offset = next % traceBuffer.length;
        traceBuffer[offset].init(next, msg);
    }

    public void event(int msg, int d1) {
        if (!enabled) 
            return;
        int next = seq.getNext();
        int offset = next % traceBuffer.length;
        traceBuffer[offset].init(next, msg, d1);
    }

    public void event(int msg, int d1, int d2) {
        if (!enabled) 
            return;
        int next = seq.getNext();
        int offset = next % traceBuffer.length;
        traceBuffer[offset].init(next, msg, d1, d2);
    }

    public void event(int msg, int d1, int d2, int d3) {
        if (!enabled) 
            return;
        int next = seq.getNext();
        int offset = next % traceBuffer.length;
        traceBuffer[offset].init(next, msg, d1, d2, d3);
    }

    public void event(int msg, int d1, int d2, int d3, int d4) {
        if (!enabled) 
            return;
        int next = seq.getNext();
        int offset = next % traceBuffer.length;
        traceBuffer[offset].init(next, msg, d1, d2, d3, d4);
    }

    /**
     * Dumps the contents of the trace buffer to the logger named
     * org.simpledbm.rss.trace. Messages are output only if this logger has a
     * level of DEBUG or higher.
     */
    public void visitall(TraceVisitor visitor) {
        /*
         * As the trace buffer can change while we are dumping its contents, we
         * need some way to decide which messages to output. At present we
         * simply check the current sequence number, and dump all messages with
         * a sequence number less than the one we noted. This has a problem
         * though - if the sequence number wraps around, and dump() is invoked,
         * then any messages that have a sequence number greater than the noted
         * sequence will not get output. To work around this issue, we also dump
         * messages that have a sequence number that is much greater than the
         * current max - to be exact, if the difference is greater than
         * Integer.MAX_VALUE/2.
         */
        // int max = seq.get();
        // for (int i = 0; i < traceBuffer.length; i++) {
        // TraceElement e = traceBuffer[i];
        // if ((e.seq < max || (e.seq > max && ((e.seq - max) >
        // Integer.MAX_VALUE/2))) && e.msg != -1) {
        // if (e.msg < 0 || e.msg > messages.length) {
        // log.debug(Trace.class.getName(), "dump",
        // MessageFormat.format(defaultMessage,
        // e.tid, e.seq, e.d1, e.d2, e.d3, e.d4, e.msg));
        // }
        // else {
        // String msg = messages[e.msg];
        // log.debug(Trace.class.getName(), "dump", MessageFormat.format(msg,
        // e.tid, e.seq, e.d1, e.d2, e.d3, e.d4));
        // }
        // }
        // }
        if (!enabled) 
            return;
        for (int i = 0; i < traceBuffer.length; i++) {
            TraceElement e = traceBuffer[i];
            visitor.visit(e.tid, e.seq, e.msg, e.d1, e.d2, e.d3, e.d4);
        }
    }

    static final class TraceElement {
        volatile long tid;
        volatile int seq;
        volatile int msg = -1;
        volatile int d1;
        volatile int d2;
        volatile int d3;
        volatile int d4;

        void init(int seq, int msg) {
            this.seq = seq;
            this.tid = Thread.currentThread().getId();
            this.msg = msg;
            this.d1 = this.d2 = this.d3 = this.d4 = 0;
        }

        void init(int seq, int msg, int d1) {
            this.seq = seq;
            this.tid = Thread.currentThread().getId();
            this.msg = msg;
            this.d1 = d1;
            this.d2 = this.d3 = this.d4 = 0;
        }

        void init(int seq, int msg, int d1, int d2) {
            this.seq = seq;
            this.tid = Thread.currentThread().getId();
            this.msg = msg;
            this.d1 = d1;
            this.d2 = d2;
            this.d3 = this.d4 = 0;
        }

        void init(int seq, int msg, int d1, int d2, int d3) {
            this.seq = seq;
            this.tid = Thread.currentThread().getId();
            this.msg = msg;
            this.d1 = d1;
            this.d2 = d2;
            this.d3 = d3;
            this.d4 = 0;
        }

        void init(int seq, int msg, int d1, int d2, int d3, int d4) {
            this.seq = seq;
            this.tid = Thread.currentThread().getId();
            this.msg = msg;
            this.d1 = d1;
            this.d2 = d2;
            this.d3 = d3;
            this.d4 = d4;
        }
    }
}
