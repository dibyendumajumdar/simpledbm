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
package org.simpledbm.common.util.logging;

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
     * Sets the diagnostics level. A value higher than 0 enables special debug
     * messages.
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
