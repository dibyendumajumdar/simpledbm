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
package org.simpledbm.network.nio;

import org.simpledbm.junit.BaseTestCase;
import org.simpledbm.network.nio.api.Connection;
import org.simpledbm.network.nio.api.NetworkServer;
import org.simpledbm.network.nio.api.NetworkUtil;
import org.simpledbm.network.nio.api.Response;
import org.simpledbm.network.nio.samples.EchoRequestHandler;

import java.util.Properties;

public class NetworkTest extends BaseTestCase {

    public NetworkTest(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testBasics() {

        NetworkServer networkServer = NetworkUtil.createNetworkServer(platform, new EchoRequestHandler(), new Properties());
        networkServer.start();
        Thread t = new Thread(new TestClient(networkServer));
        t.start();
        try {
            Thread.sleep(1000);
            while (networkServer.isOpen()) {
                networkServer.select();
            }
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            networkServer.shutdown();
        }
    }

    final class TestClient implements Runnable {

        NetworkServer server;

        public TestClient(NetworkServer server) {
            super();
            this.server = server;
        }

        public void run() {

            try {
                Thread.sleep(500);
            } catch (InterruptedException e1) {
            }

            Connection c = null;
            try {
                System.err.println("Connecting ...");
                c = NetworkUtil.createConnection("localhost", 8000, 10000);
                for (int i = 0; i < 5; i++) {
                    System.err.println("Connected, sending message");
                    Response response = c.submit(NetworkUtil.createRequest("hello world!"));
                    String str = new String(response.getData().array());
                    System.err.println("Received reply [" + str + "]");
                    if (!str.equals("hello world!")) {
                         throw new RuntimeException("Unexpected response from server");
                    }
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                }
            } catch (Exception e) {
                NetworkTest.super.setThreadFailed(Thread.currentThread(), e);
            }
            finally {
                if (c != null) {
                    c.close();
                }
                server.requestStop();
            }
        }
    }
}
