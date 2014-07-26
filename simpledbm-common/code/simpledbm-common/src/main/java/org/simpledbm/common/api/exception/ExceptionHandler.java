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
package org.simpledbm.common.api.exception;

import org.simpledbm.common.tools.diagnostics.Trace;
import org.simpledbm.common.util.logging.Logger;

/**
 * The ExceptionHandler mechanism allows the raising of exception to be handled
 * by a common piece of code. The primary advantage is that information can be
 * dumped to the logs automatically when an exception is raised.
 * 
 * @author dibyendu majumdar
 */
public interface ExceptionHandler {

    /**
     * Logs an error message and throws the supplied exception. The exception
     * will be logged with the error message.
     * 
     * @param sourceClass
     *            The name of the class where the exception was raised
     * @param sourceMethod
     *            The method where the exception was raised
     * @param e
     *            The exception to throw
     */
    public void errorThrow(Class<?> sourceClass, String sourceMethod,
            SimpleDBMException e);

    /**
     * Logs an error message and throws the supplied exception. The exception
     * will be logged with the error message. Also dumps {@link Trace} messages.
     * 
     * @param sourceClass
     *            The name of the class where the exception was raised
     * @param sourceMethod
     *            The method where the exception was raised
     * @param e
     *            The exception to throw
     */
    public void unexpectedErrorThrow(Class<?> sourceClass, String sourceMethod,
            SimpleDBMException e);

    /**
     * Logs a warning message and throws the supplied exception. The exception
     * will be logged with the warning message.
     * 
     * @param sourceClass
     *            The name of the class where the exception was raised
     * @param sourceMethod
     *            The method where the exception was raised
     * @param e
     *            The exception to throw
     */
    public void warnAndThrow(Class<?> sourceClass, String sourceMethod,
            SimpleDBMException e);

    /**
     * Throws the supplied exception. The exception will normally not be logged.
     * 
     * @param sourceClass
     *            The name of the class where the exception was raised
     * @param sourceMethod
     *            The method where the exception was raised
     * @param e
     *            The exception to throw
     */
    public void throwSilently(Class<?> sourceClass, String sourceMethod,
            SimpleDBMException e);
}
