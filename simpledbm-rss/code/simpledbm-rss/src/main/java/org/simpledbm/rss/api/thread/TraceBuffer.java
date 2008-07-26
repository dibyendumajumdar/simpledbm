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
package org.simpledbm.rss.api.thread;

import org.simpledbm.rss.impl.thread.TraceBufferImpl;

/**
 * A TraceBuffer is an efficient thread safe but lock free ring buffer which 
 * can be used to log trace message in memory. As the buffer is lock free it
 * should minimise side effects. Messages are stored in memory so that there is
 * very little performance impact. Each message is tagged with the thread id,
 * and a sequence number. Note that the sequence number wraps around when it
 * reaches 2147483646. 
 * <p>
 * Trace messages can be dumped to the log by invoking dump().
 * <p>
 * The design of TraceBuffer was inspired by the article in DDJ April 23, 2007,
 * Multi-threaded Debugging Techniques by Shameem Akhter and Jason Roberts.
 * This is an excerpt from the book Multi-Core Programming by the same authors.
 * 
 * @author dibyendu majumdar
 * @since 26 July 2008
 */
public abstract class TraceBuffer {

	public static TraceBuffer getTraceBuffer(int size) {
		return new TraceBufferImpl(size);
	}
	
	public abstract void put(Object o);
	
	public abstract void dump();
	
}
