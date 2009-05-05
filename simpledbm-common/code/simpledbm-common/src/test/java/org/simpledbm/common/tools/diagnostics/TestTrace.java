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
package org.simpledbm.common.tools.diagnostics;

import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.tools.diagnostics.Trace;
import org.simpledbm.common.tools.diagnostics.TraceBuffer;
import org.simpledbm.junit.BaseTestCase;

public class TestTrace extends BaseTestCase {

	public TestTrace() {
	}

	public TestTrace(String arg0) {
		super(arg0);
	}

	public void testBasics() {
		String traceMessages = "traceMessages.txt";
		PlatformObjects po = platform.getPlatformObjects("org.simpledbm.trace");
		po.getLogger().enableDebug();
		TraceBuffer traceBuffer = po.getTraceBuffer();
		traceBuffer.event(1);
		traceBuffer.event(20);
		Trace trace = new Trace(traceBuffer, po.getLogger(), traceMessages);
		trace.dump();
	}
	
}
