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
package org.simpledbm.network.client;

import org.simpledbm.junit.BaseTestCase;
import org.simpledbm.network.client.api.Session;
import org.simpledbm.network.client.api.SessionManager;
import org.simpledbm.network.nio.api.Connection;
import org.simpledbm.network.nio.api.NetworkServer;
import org.simpledbm.network.nio.api.NetworkUtil;
import org.simpledbm.network.nio.api.Response;
import org.simpledbm.network.nio.samples.EchoRequestHandler;
import org.simpledbm.network.server.SimpleDBMServer;
import org.simpledbm.typesystem.api.TypeFactory;

import java.util.Properties;

public class ClientTest extends BaseTestCase {

    public ClientTest(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
        SimpleDBMServer server = new SimpleDBMServer();
        server.run(new String[]{"create", "test.properties"});
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    static class MyServer implements Runnable {

        volatile boolean stop = false;
        SimpleDBMServer server = new SimpleDBMServer();

        MyServer() {
        }

        public void run() {
            System.err.println("starting server");
            try {
                server.run(new String[]{"open", "test.properties"});
                while (!stop) {
                    server.select();
                }
                server.shutdown();
            }
            catch (Exception e) {
                System.err.println("failed to start server");
                e.printStackTrace();
            }
        }

        void shutdown() {
            stop = true;
        }
    }

    public void testConnection() {
        MyServer server = new MyServer();
        Thread t = new Thread(server);
        t.start();

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
        }

        SessionManager sessionManager = new SessionManager("localhost", 8000);
        TypeFactory typeFactory = sessionManager.getTypeFactory();
        Session session = sessionManager.openSession();
        server.shutdown();

        try {
            t.join();
        } catch (InterruptedException e) {
        }
    }

}
