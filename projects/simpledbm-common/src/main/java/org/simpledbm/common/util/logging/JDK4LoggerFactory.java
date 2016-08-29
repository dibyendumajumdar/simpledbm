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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.simpledbm.common.util.container.ConfigProperties;
import org.simpledbm.common.util.logging.Logger;
import org.simpledbm.common.util.logging.LoggerFactory;

/**
 * A simple wrapper around Log4J/JDK logging facilities.
 * 
 * @author Dibyendu Majumdar
 */
public class JDK4LoggerFactory implements LoggerFactory {

    /**
     * Configures the logging system using properties in the supplied
     * properties. Two properties are supported:
     * <dl>
     * <dt>logging.properties.file</dt>
     * <dd>Configuration file. If the filename is prefixed by
     * &quot;classpath:&quot;, the file must exist in the classpath, else it
     * must exist on the specified location on the filesystem.</dd>
     * <dt>logging.properties.type</dt>
     * <dd>If the type is set to <tt>log4j</tt>, the logging system uses log4j,
     * else it uses jdk4 logging.</dd>
     * </dl>
     */
    public JDK4LoggerFactory(ConfigProperties properties) {
        String logFile = properties.getProperty("logging.properties.file",
                "classpath:simpledbm.logging.properties");
        configureJDKLogging(logFile);
    }

    void configureJDKLogging(String name) {
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
                is = Thread.currentThread().getContextClassLoader()
                        .getResourceAsStream(filename);
            } else {
                is = new FileInputStream(filename);
            }
            java.util.logging.LogManager.getLogManager().readConfiguration(is);
        } catch (Exception e) {
            System.err
                    .println("SIMPLEDBM-WL0001: Failed to initialize JDK 1.4 logging system due to following error:"
                            + e.getMessage());
        } finally {
            try {
                if (is != null) {
                    is.close();
                }
            } catch (IOException e) {
            }
        }
    }

    public Logger getLogger(String name) {
        return new JDK4Logger(name);
    }
}
