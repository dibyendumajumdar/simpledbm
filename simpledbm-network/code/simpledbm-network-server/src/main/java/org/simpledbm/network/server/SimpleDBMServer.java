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

    Platform platform;
    
    NetworkServer networkServer = null;

    private void usage() {
        System.out.println("SimpleDBMServer create|open <properties-file>");
    }

    public void run(String[] args) {
        if (args.length != 2) {
            usage();
            System.exit(1);
        }
        Properties properties = parseProperties(args[1]);
        if (properties == null) {
            System.exit(1);
        }
        String command = args[0];
        if ("create".equalsIgnoreCase(command)) {
            create(properties);
        } else if ("open".equalsIgnoreCase(command)) {
            open(properties);
        } else {
            usage();
            System.exit(1);
        }
    }

    private void create(Properties properties) {
        DatabaseFactory.create(properties);
    }

    private void open(Properties properties) {
        platform = new PlatformImpl(properties);
        SimpleDBMRequestHandler simpleDBMRequestHandler = new SimpleDBMRequestHandler();
        networkServer = NetworkUtil.createNetworkServer(platform,
                simpleDBMRequestHandler, properties);
        Runtime.getRuntime().addShutdownHook(
                new MyShutdownThread(networkServer));
        networkServer.start();
    }

    public void select() {
        networkServer.select();
    }

    public void shutdown() {
        if (networkServer != null) {
            networkServer.shutdown();
            networkServer = null;
        }
        platform.shutdown();
    }

    private Properties parseProperties(String arg) {
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
        final NetworkServer server;

        MyShutdownThread(NetworkServer server) {
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
        SimpleDBMServer server = new SimpleDBMServer();
        server.run(args);
    }
}
