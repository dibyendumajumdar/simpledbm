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
