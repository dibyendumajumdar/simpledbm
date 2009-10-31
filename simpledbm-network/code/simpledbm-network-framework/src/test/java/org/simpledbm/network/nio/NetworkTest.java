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
package org.simpledbm.network.nio;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import org.simpledbm.network.nio.impl.ClientConnection;
import org.simpledbm.network.nio.impl.NetworkServer;

import junit.framework.TestCase;

public class NetworkTest extends TestCase {

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

		NetworkServer networkServer = new NetworkServer("localhost", 8000);
        System.err.println(networkServer);
        networkServer.open();
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
		}
		networkServer.close();
	}
	
	static final class TestClient implements Runnable {

		NetworkServer server;
		
		public TestClient(NetworkServer server) {
			super();
			this.server = server;
		}

		public void run() {
			
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			Socket s = null;
			ClientConnection c = null;
			try {
				System.err.println("Connecting ...");
				s = new Socket("localhost", 8000);
				c = new ClientConnection(s);
				System.err.println("Connected, sending message");
				c.write("hello world!".getBytes());	
				byte[] data = c.read();
				String str = new String(data);
				System.err.println("Received message [" + str +"]");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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
