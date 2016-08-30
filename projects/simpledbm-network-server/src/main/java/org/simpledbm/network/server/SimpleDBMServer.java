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
package org.simpledbm.network.server;

import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.impl.platform.PlatformImpl;
import org.simpledbm.database.api.DatabaseFactory;
import org.simpledbm.network.nio.api.NetworkServer;
import org.simpledbm.network.nio.api.NetworkUtil;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class SimpleDBMServer {

    final Properties properties;

    Platform platform;

    NetworkServer networkServer = null;

    volatile boolean stop = false;

    volatile boolean started = false;

    private static void usage() {
        System.out.println("SimpleDBMServer create|open <properties-file>");
    }

    private static void run(String[] args) {
        String command = args[0];
        if ("create".equalsIgnoreCase(command)) {
            create(parseProperties(args[1]));
        } else if ("open".equalsIgnoreCase(command)) {
            SimpleDBMServer server = new SimpleDBMServer(args[1]);
            try {
                server.open();
                while (!server.stop) {
                    server.select();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                server.shutdown();
            }
        } else {
            usage();
            System.exit(1);
        }
    }

    public static void create(Properties properties) {
        DatabaseFactory.create(properties);
    }

    public SimpleDBMServer(String propertiesFile) {
        properties = parseProperties(propertiesFile);
        started = false;
        stop = false;
    }

    public synchronized void open() {
        if (started) {
            return;
        }
        platform = new PlatformImpl(properties);
        SimpleDBMRequestHandler simpleDBMRequestHandler = new SimpleDBMRequestHandler();
        networkServer = NetworkUtil.createNetworkServer(platform,
                simpleDBMRequestHandler, properties);
        Runtime.getRuntime().addShutdownHook(new MyShutdownThread(this));
        networkServer.start();
        started = true;
    }

    public synchronized void select() {
        if (!started) {
            return;
        }
        networkServer.select();
    }

    public void shutdown() {
        if (!started) {
            return;
        }
        stop = true;
        synchronized (this) {
            if (networkServer != null) {
                networkServer.shutdown();
                networkServer = null;
            }
            platform.shutdown();
            started = false;
        }
    }

    private static Properties parseProperties(String arg) {
        InputStream in = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(arg);
        if (null == in) {
            try {
                in = new FileInputStream(arg);
            } catch (FileNotFoundException e) {
            }
        }
        if (null == in) {
            System.err.println("Unable to access resource [" + arg + "]");
            return null;
        }
        Properties properties = new Properties();
        try {
            properties.load(in);
        } catch (IOException e) {
            System.err.println("Error loading from resource [" + arg + "] :"
                    + e.getMessage());
            return null;
        } finally {
            try {
                in.close();
            } catch (IOException ignored) {
            }
        }
        System.out.println(properties);
        return properties;
    }

    public static class MyShutdownThread extends Thread {
        final SimpleDBMServer server;

        MyShutdownThread(SimpleDBMServer server) {
            super();
            this.server = server;
        }

        public void run() {
            if (server != null) {
                server.shutdown();
            }
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            usage();
            System.exit(1);
        }
        run(args);
    }
}
