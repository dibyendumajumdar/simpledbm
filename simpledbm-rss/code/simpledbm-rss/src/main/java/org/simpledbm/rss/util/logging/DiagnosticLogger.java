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
 *    Email  : dibyendu@mazumdar.demon.co.uk
 */
package org.simpledbm.rss.util.logging;

public final class DiagnosticLogger {
	
	private static final ThreadLocal<LogHandler> stream = new ThreadLocal<LogHandler>();
    private static int diagnosticsLevel = 0;
    private static LogHandler defaultHandler = new DefaultHandler();
	
	public static void setHandler(LogHandler out) {
		stream.set(out);
	}

	public static LogHandler getLogHandler() {
		LogHandler handler = stream.get();
		if (handler == null) {
			handler = defaultHandler;
		}
		return handler;
	}
	
	public static void log(String string) {
		if (diagnosticsLevel == 0) {
			return;
		}
		LogHandler os = getLogHandler();
		StringBuilder sb = new StringBuilder();
		sb.append(Thread.currentThread().getName());
		sb.append(": ");
		sb.append(string);
		os.log(sb.toString());
	}
	
	/**
	 * Gets current value of diagnostics level.
	 */
	public static final int getDiagnosticsLevel() {
		return diagnosticsLevel;
	}

	/**
	 * Sets the diagnostics level. A value higher than 0 enables special 
	 * debug messages.
	 */
	public static final void setDiagnosticsLevel(int diagnosticsLevel) {
		DiagnosticLogger.diagnosticsLevel = diagnosticsLevel;
	}	
	
	public static interface LogHandler {
		void log(String s);
	}

	static class DefaultHandler implements LogHandler {
		public void log(String s) {
			System.err.println(s);
		}
	}
}
