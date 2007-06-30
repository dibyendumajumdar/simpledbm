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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.LogManager;

import org.simpledbm.rss.util.ClassUtils;
import org.simpledbm.rss.util.mcat.MessageCatalog;

/**
 * A simple wrapper around JDK logging facilities. The aim is to allow 
 * easy switch to another logging system, such as Log4J.
 * 
 * @author Dibyendu Majumdar
 */
public final class Logger {

    /**
     * Instance of the real logger object.
     */
    private final java.util.logging.Logger realLogger;

    private static final MessageCatalog mcat = new MessageCatalog();

    /**
     * Obtain a new or existing Logger instance. 
     * @param name Name of the logger, package names are recommended
     */
    public static Logger getLogger(String name) {
        return new Logger(name);
    }

    /**
     * Configures the logging system using properties in the supplied file.
     * If the filename is prefixed by &quot;classpath:&quot;, the file must exist
     * in the classpath, else it must exist on the specified location on the filesystem.
     * 
     * @param name Name of the logging properties file, optionally prefixed by &quot;classpath:&quot; 
     */
    public static void configure(String name) {
        final String classpathPrefix = "classpath:";
        boolean searchClasspath = false;
        String filename = name;
        if (filename.startsWith(classpathPrefix)) {
            filename = filename.substring(classpathPrefix.length());
            searchClasspath = true;
        }
        InputStream is = null;
        try {
            if (searchClasspath) {
                is = ClassUtils.getResourceAsStream(filename);
            } else {
                is = new FileInputStream(filename);
            }
            LogManager.getLogManager().readConfiguration(is);
        } catch (Exception e) {
            System.err.println(mcat.getMessage("WL0001") + e.getMessage());
        } finally {
            try {
                if (is != null) {
                    is.close();
                }
            } catch (IOException e) {
            }
        }
    }

    public Logger(String name) {
        realLogger = java.util.logging.Logger.getLogger(name);
    }

    public void info(String sourceClass, String sourceMethod, String message) {
        realLogger.logp(Level.INFO, sourceClass, sourceMethod, message);
    }

    public void info(String sourceClass, String sourceMethod, String message,
            Object... args) {
        realLogger.logp(Level.INFO, sourceClass, sourceMethod, message, args);
    }

    public void info(String sourceClass, String sourceMethod, String message,
            Throwable thrown) {
        realLogger.logp(Level.INFO, sourceClass, sourceMethod, message, thrown);
    }

    public void debug(String sourceClass, String sourceMethod, String message) {
        realLogger.logp(Level.FINE, sourceClass, sourceMethod, message);
    }

    public void debug(String sourceClass, String sourceMethod, String message,
            Object... args) {
        realLogger.logp(Level.FINE, sourceClass, sourceMethod, message, args);
    }

    public void debug(String sourceClass, String sourceMethod, String message,
            Throwable thrown) {
        realLogger.logp(Level.FINE, sourceClass, sourceMethod, message, thrown);
    }

    public void trace(String sourceClass, String sourceMethod, String message) {
        realLogger.logp(Level.FINER, sourceClass, sourceMethod, message);
    }

    public void trace(String sourceClass, String sourceMethod, String message,
            Object... args) {
        realLogger.logp(Level.FINER, sourceClass, sourceMethod, message, args);
    }

    public void trace(String sourceClass, String sourceMethod, String message,
            Throwable thrown) {
        realLogger
            .logp(Level.FINER, sourceClass, sourceMethod, message, thrown);
    }

    public void warn(String sourceClass, String sourceMethod, String message) {
        realLogger.logp(Level.WARNING, sourceClass, sourceMethod, message);
    }

    public void warn(String sourceClass, String sourceMethod, String message,
            Object... args) {
        realLogger
            .logp(Level.WARNING, sourceClass, sourceMethod, message, args);
    }

    public void warn(String sourceClass, String sourceMethod, String message,
            Throwable thrown) {
        realLogger.logp(
            Level.WARNING,
            sourceClass,
            sourceMethod,
            message,
            thrown);
    }

    public void error(String sourceClass, String sourceMethod, String message) {
        realLogger.logp(Level.SEVERE, sourceClass, sourceMethod, message);
    }

    public void error(String sourceClass, String sourceMethod, String message,
            Object... args) {
        realLogger.logp(Level.SEVERE, sourceClass, sourceMethod, message, args);
    }

    public void error(String sourceClass, String sourceMethod, String message,
            Throwable thrown) {
        realLogger.logp(
            Level.SEVERE,
            sourceClass,
            sourceMethod,
            message,
            thrown);
    }

    public boolean isTraceEnabled() {
        return realLogger.isLoggable(Level.FINER);
    }

    public boolean isDebugEnabled() {
        return realLogger.isLoggable(Level.FINE);
    }

    public void enableDebug() {
        realLogger.setLevel(Level.FINE);
    }

    public void disableDebug() {
        realLogger.setLevel(Level.INFO);
    }

}
