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

import org.simpledbm.common.util.logging.Logger;


/**
 * A simple wrapper around Log4J/JDK logging facilities.
 * 
 * @author Dibyendu Majumdar
 */
public class JDK4Logger implements Logger {

    private java.util.logging.Logger realLogger;

    public JDK4Logger(String name) {
        realLogger = java.util.logging.Logger.getLogger(name);
    }

    public void info(Class<?> sourceClass, String sourceMethod, String message) {
        realLogger.logp(java.util.logging.Level.INFO, sourceClass.getName(),
                sourceMethod, message);
    }

    public void info(Class<?> sourceClass, String sourceMethod, String message,
            Throwable thrown) {
        realLogger.logp(java.util.logging.Level.INFO, sourceClass.getName(),
                sourceMethod, message, thrown);
    }

    public void debug(Class<?> sourceClass, String sourceMethod, String message) {
        realLogger.logp(java.util.logging.Level.FINE, sourceClass.getName(),
                sourceMethod, message);
    }

    public void debug(Class<?> sourceClass, String sourceMethod, String message,
            Throwable thrown) {
        realLogger.logp(java.util.logging.Level.FINE, sourceClass.getName(),
                sourceMethod, message, thrown);
    }

    public void trace(Class<?> sourceClass, String sourceMethod, String message) {
        realLogger.logp(java.util.logging.Level.FINER, sourceClass.getName(),
                sourceMethod, message);
    }

    public void trace(Class<?> sourceClass, String sourceMethod, String message,
            Throwable thrown) {
        realLogger.logp(java.util.logging.Level.FINER, sourceClass.getName(),
                sourceMethod, message, thrown);
    }

    public void warn(Class<?> sourceClass, String sourceMethod, String message) {
        realLogger.logp(java.util.logging.Level.WARNING, sourceClass.getName(),
                sourceMethod, message);
    }

    public void warn(Class<?> sourceClass, String sourceMethod, String message,
            Throwable thrown) {
        realLogger.logp(java.util.logging.Level.WARNING, sourceClass.getName(),
                sourceMethod, message, thrown);
    }

    public void error(Class<?> sourceClass, String sourceMethod, String message) {
        realLogger.logp(java.util.logging.Level.SEVERE, sourceClass.getName(),
                sourceMethod, message);
    }

    public void error(Class<?> sourceClass, String sourceMethod, String message,
            Throwable thrown) {
        realLogger.logp(java.util.logging.Level.SEVERE, sourceClass.getName(),
                sourceMethod, message, thrown);
    }

    public boolean isTraceEnabled() {
        return realLogger.isLoggable(java.util.logging.Level.FINER);
    }

    public boolean isDebugEnabled() {
        return realLogger.isLoggable(java.util.logging.Level.FINE)
                || realLogger.isLoggable(java.util.logging.Level.FINER);
    }

    public void enableDebug() {
        realLogger.setLevel(java.util.logging.Level.FINE);
    }

    public void disableDebug() {
        realLogger.setLevel(java.util.logging.Level.INFO);
    }

}
