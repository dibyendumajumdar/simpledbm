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
package org.simpledbm.common.util.logging;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * A simple wrapper around Log4J/JDK logging facilities.
 *
 * @author Dibyendu Majumdar
 */
public abstract class Logger {

    /**
     * LoggerFactory for creating Loggers. By default, a JDK1.4 Logger
     * Factory will be used.
     */
    private static LoggerFactory loggerFactory = new Jdk4LoggerFactory();

    /**
     * Obtain a new or existing Logger instance.
     * @param name Name of the logger, package names are recommended
     */
    public static Logger getLogger(String name) {
        return loggerFactory.getLogger(name);
    }

    private static boolean log4JAvailable() {
        try {
            Class.forName("org.apache.log4j.PropertyConfigurator");
        } catch (ClassNotFoundException e) {
            return false;
        }
        return true;
    }

    /**
     * Configures the logging system using properties in the supplied properties.
     * Two properties are supported:
     * <dl>
     * <dt>logging.properties.file</dt>
     * <dd>Configuration file. If the filename is prefixed by &quot;classpath:&quot;, the file must exist
     * in the classpath, else it must exist on the specified location on the filesystem.</dd>
     * <dt>logging.properties.type</dt>
     * <dd>If the type is set to <tt>log4j</tt>, the logging system uses log4j, else it
     * uses jdk4 logging.</dd>
     * </dl>
     */
    public static void configure(Properties properties) {
        String logFile = properties.getProperty("logging.properties.file", "classpath:simpledbm.logging.properties");
        String logType = properties.getProperty("logging.properties.type", "log4j");
        if ("log4j".equalsIgnoreCase(logType) && log4JAvailable()) {
            Logger.configureLog4JLogging(logFile);
        } else {
            Logger.configureJDKLogging(logFile);
        }
    }

    static void configureLog4JLogging(String name) {
        loggerFactory = new Log4JLoggerFactory();
        final String classpathPrefix = "classpath:";
        boolean searchClasspath = false;
        boolean isXml = false;
        String filename = name;
        if (filename.startsWith(classpathPrefix)) {
            filename = filename.substring(classpathPrefix.length());
            searchClasspath = true;
        }
        if (filename.endsWith(".xml")) {
            isXml = true;
        }
        if (searchClasspath) {
            URL url = Thread.currentThread().getContextClassLoader().getResource(filename);
            if (url == null) {
                System.err.println("SIMPLEDBM-WL0002: Failed to initialize Log4J logging system");
            }
            if (isXml) {
                org.apache.log4j.xml.DOMConfigurator.configure(url);
            } else {
                org.apache.log4j.PropertyConfigurator.configure(url);
            }
        } else {
            if (isXml) {
                org.apache.log4j.xml.DOMConfigurator.configure(filename);
            } else {
                org.apache.log4j.PropertyConfigurator.configure(filename);
            }
        }
    }

    static void configureJDKLogging(String name) {
        loggerFactory = new Jdk4LoggerFactory();
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
                is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename);
            } else {
                is = new FileInputStream(filename);
            }
            java.util.logging.LogManager.getLogManager().readConfiguration(is);
        } catch (Exception e) {
            System.err.println("SIMPLEDBM-WL0001: Failed to initialize JDK 1.4 logging system due to following error:" + e.getMessage());
        } finally {
            try {
                if (is != null) {
                    is.close();
                }
            } catch (IOException e) {
            }
        }
    }

    public abstract void info(String sourceClass, String sourceMethod, String message);

    public abstract void info(String sourceClass, String sourceMethod, String message, Throwable t);

    public abstract void debug(String sourceClass, String sourceMethod, String message);

    public abstract void debug(String sourceClass, String sourceMethod, String message, Throwable thrown);

    public abstract void trace(String sourceClass, String sourceMethod, String message);

    public abstract void trace(String sourceClass, String sourceMethod, String message, Throwable thrown);

    public abstract void warn(String sourceClass, String sourceMethod, String message);

    public abstract void warn(String sourceClass, String sourceMethod, String message, Throwable thrown);

    public abstract void error(String sourceClass, String sourceMethod, String message);

    public abstract void error(String sourceClass, String sourceMethod, String message, Throwable thrown);

    public abstract boolean isTraceEnabled();

    public abstract boolean isDebugEnabled();

    public abstract void enableDebug();

    public abstract void disableDebug();

    static interface LoggerFactory {

        Logger getLogger(String name);
    }

    static class Log4JLoggerFactory implements LoggerFactory {

        public Logger getLogger(String name) {
            return new Log4JLogger(name);
        }
    }

    static class Jdk4LoggerFactory implements LoggerFactory {

        public Logger getLogger(String name) {
            return new Jdk4Logger(name);
        }
    }

    static final class Jdk4Logger extends Logger {

        private java.util.logging.Logger realLogger;

        public Jdk4Logger(String name) {
            realLogger = java.util.logging.Logger.getLogger(name);
        }

        public void info(String sourceClass, String sourceMethod, String message) {
            realLogger.logp(java.util.logging.Level.INFO, sourceClass, sourceMethod, message);
        }

        public void info(String sourceClass, String sourceMethod, String message, Throwable thrown) {
            realLogger.logp(java.util.logging.Level.INFO, sourceClass, sourceMethod, message, thrown);
        }

        public void debug(String sourceClass, String sourceMethod, String message) {
            realLogger.logp(java.util.logging.Level.FINE, sourceClass, sourceMethod, message);
        }

        public void debug(String sourceClass, String sourceMethod, String message, Throwable thrown) {
            realLogger.logp(java.util.logging.Level.FINE, sourceClass, sourceMethod, message, thrown);
        }

        public void trace(String sourceClass, String sourceMethod, String message) {
            realLogger.logp(java.util.logging.Level.FINER, sourceClass, sourceMethod, message);
        }

        public void trace(String sourceClass, String sourceMethod, String message, Throwable thrown) {
            realLogger.logp(java.util.logging.Level.FINER, sourceClass, sourceMethod, message, thrown);
        }

        public void warn(String sourceClass, String sourceMethod, String message) {
            realLogger.logp(java.util.logging.Level.WARNING, sourceClass, sourceMethod, message);
        }

        public void warn(String sourceClass, String sourceMethod, String message, Throwable thrown) {
            realLogger.logp(java.util.logging.Level.WARNING, sourceClass, sourceMethod, message, thrown);
        }

        public void error(String sourceClass, String sourceMethod, String message) {
            realLogger.logp(java.util.logging.Level.SEVERE, sourceClass, sourceMethod, message);
        }

        public void error(String sourceClass, String sourceMethod, String message, Throwable thrown) {
            realLogger.logp(java.util.logging.Level.SEVERE, sourceClass, sourceMethod, message, thrown);
        }

        public boolean isTraceEnabled() {
            return realLogger.isLoggable(java.util.logging.Level.FINER);
        }

        public boolean isDebugEnabled() {
            return realLogger.isLoggable(java.util.logging.Level.FINE) || realLogger.isLoggable(java.util.logging.Level.FINER);
        }

        public void enableDebug() {
            realLogger.setLevel(java.util.logging.Level.FINE);
        }

        public void disableDebug() {
            realLogger.setLevel(java.util.logging.Level.INFO);
        }
    }

    static final class Log4JLogger extends Logger {

        private org.apache.log4j.Logger realLogger;

        public Log4JLogger(String name) {
            realLogger = org.apache.log4j.LogManager.getLogger(name);
        }

        public void info(String sourceClass, String sourceMethod, String message) {
            realLogger.info(message);
        }

        public void info(String sourceClass, String sourceMethod, String message, Throwable t) {
            realLogger.info(message, t);
        }

        public void debug(String sourceClass, String sourceMethod, String message) {
            realLogger.debug(message);
        }

        public void debug(String sourceClass, String sourceMethod, String message, Throwable thrown) {
            realLogger.debug(message, thrown);
        }

        public void trace(String sourceClass, String sourceMethod, String message) {
            realLogger.trace(message);
        }

        public void trace(String sourceClass, String sourceMethod, String message, Throwable thrown) {
            realLogger.trace(message, thrown);
        }

        public void warn(String sourceClass, String sourceMethod, String message) {
            realLogger.warn(message);
        }

        public void warn(String sourceClass, String sourceMethod, String message, Throwable thrown) {
            realLogger.warn(message, thrown);
        }

        public void error(String sourceClass, String sourceMethod, String message) {
            realLogger.error(message);
        }

        public void error(String sourceClass, String sourceMethod, String message, Throwable thrown) {
            realLogger.error(message, thrown);
        }

        public boolean isTraceEnabled() {
            return realLogger.isTraceEnabled();
        }

        public boolean isDebugEnabled() {
            return realLogger.isDebugEnabled() || realLogger.isTraceEnabled();
        }

        public void enableDebug() {
            realLogger.setLevel(org.apache.log4j.Level.DEBUG);
        }

        public void disableDebug() {
            realLogger.setLevel(org.apache.log4j.Level.INFO);
        }
    }
}