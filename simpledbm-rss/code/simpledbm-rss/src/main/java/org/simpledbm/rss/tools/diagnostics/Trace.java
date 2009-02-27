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
package org.simpledbm.rss.tools.diagnostics;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.MessageFormat;
import java.util.ArrayList;

import org.simpledbm.rss.api.exception.RSSException;
import org.simpledbm.rss.util.logging.Logger;

/**
 * An efficient thread safe but lock free mechanism to generate trace messages.
 * Uses a ring buffer.  Messages are stored in memory so that there is very
 * little performance impact. Each message is tagged with the thread id, and a
 * sequence number. The sequence number wraps around when it reaches
 * 2147483646.
 * <p>
 * Trace messages can be dumped to the log by invoking dump(). The messages are
 * only output if a logger named org.simpledbm.rss.trace has level set to DEBUG.
 * <p>
 * The design of Trace mechanism was inspired by the article in <cite>DDJ April 23, 2007,
 * Multi-threaded Debugging Techniques by Shameem Akhter and Jason Roberts</cite>. This
 * article is an excerpt from the book <cite>Multi-Core Programming</cite> by the same authors.
 * 
 * @author dibyendu majumdar
 * @since 26 July 2008
 */
public class Trace implements TraceBuffer.TraceVisitor {

	final Logger log;

	/**
	 * Default message format.
	 */
	static String defaultMessage;
	
	final String[] messages;	

	final TraceBuffer traceBuffer;
	
	/*
	 * For performance reasons, we only allow numeric arguments to be supplied
	 * as trace message identifiers and arguments. This avoids expensive object allocations.
	 * The trace message is inserted at the next position in the traceBuffer
	 * array, the next pointer wraps to the beginning of the array when it reaches 
	 * the end.
	 */

	public Trace(TraceBuffer traceBuffer, Logger log, String messageFile) {
		this.log = log;
		this.messages = loadMessages(messageFile);
		this.traceBuffer = traceBuffer;
	}

	private InputStream getResourceAsStream(String name) {
		ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream is = null;
        is = cl.getResourceAsStream(name);
        if (is == null) {
            throw new RSSException();
        }
        return is;
	}
	
	private String[] loadMessages(String resourceName) {
		InputStream is = getResourceAsStream(resourceName);
		ArrayList<String> messageList = new ArrayList<String>();
		try {
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(is));
			String line = reader.readLine();
			while (line != null) {
				messageList.add(line);
				line = reader.readLine();
			}
		} catch (IOException e) {
		} finally {
			try {
				is.close();
			} catch (IOException e) {
			}
		}
		return messageList.toArray(new String[0]);
	}
	
	public void dump() {
		if (!log.isDebugEnabled()) {
			return;
		}
		traceBuffer.visitall(this);
	}
	
	/**
	 * Dumps the contents of the trace buffer to the logger named
	 * org.simpledbm.rss.trace. Messages are output only if this logger has
	 * a level of DEBUG or higher.
	 */
	public void visit(long tid, int seq, int msg, int d1, int d2, int d3, int d4) {
		if (msg < 0 || msg > messages.length) {
		} else {
			String s = messages[msg];
			log.debug(Trace.class.getName(), "dump", MessageFormat.format(
					s, tid, seq, d1, d2, d3, d4));
		}
	}
}
