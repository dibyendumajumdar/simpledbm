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
 *    Email  : d dot majumdar at gmail dot com ignore
 */
package org.simpledbm.rss.impl.thread;

import org.simpledbm.rss.api.thread.TraceBuffer;
import org.simpledbm.rss.util.WrappingSequencer;
import org.simpledbm.rss.util.logging.Logger;

public class TraceBufferImpl extends TraceBuffer {

	static final Logger log = Logger.getLogger("org.simpledbm.rss.trace");

	final WrappingSequencer seq = new WrappingSequencer(Integer.MAX_VALUE);
	
	final TraceElement[] traceBuffer;
	
	public TraceBufferImpl(int size) {
		traceBuffer = new TraceElement[size];
	}
	
	@Override
	public void dump() {
		for (int i = 0; i < traceBuffer.length; i++) {
			TraceElement e = traceBuffer[i];
			if (e != null) {
				e.dump();
			}
		}
	}

	@Override
	public void put(Object o) {
		int next = seq.getNext();
		int offset = next % traceBuffer.length;
		traceBuffer[offset] = new TraceElement(next, o);
	}

	static final class TraceElement {
		final long tid;
		final long time;
		final Object traceE;

		TraceElement(long seq, Object traceE) {
			this.tid = Thread.currentThread().getId();
			this.time = seq;
			this.traceE = traceE;
		}

		void dump() {
			if (log.isDebugEnabled()) {
				log.debug(getClass().getName(), "dump", "SimpleDBM Trace: TID " + tid + " SEQ " + time + " MSG " + traceE.toString());
			}
		}
	}

}
