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
package org.simpledbm.rss.impl.wal;

import java.nio.ByteBuffer;

import junit.framework.TestCase;

import org.simpledbm.rss.api.wal.Lsn;

public class TestLsn extends TestCase {
	
	public TestLsn(String name) {
		super(name);
	}

	public void testLsn() throws Exception {
		Lsn lsn = new Lsn();
		assertTrue(lsn.isNull());
		Lsn lsn1 = new Lsn(1,1);
		Lsn lsn2 = new Lsn(1,0);
		Lsn lsn3 = new Lsn(1,2);
		Lsn lsn4 = new Lsn(1,1);
		Lsn lsn5 = new Lsn(0,50);
		Lsn lsn6 = new Lsn(50,0);
		assertTrue(lsn1.compareTo(lsn2) > 0);
		assertTrue(lsn1.compareTo(lsn3) < 0);
		assertTrue(lsn1.compareTo(lsn4) == 0);
		assertTrue(lsn1.compareTo(lsn5) > 0);
		assertTrue(lsn1.compareTo(lsn6) < 0);
		ByteBuffer bb = ByteBuffer.allocate(Lsn.SIZE);
		bb.rewind();
		lsn3.store(bb);
		assertFalse(bb.hasRemaining());
		bb.flip();
		lsn.retrieve(bb);
		assertTrue(lsn.compareTo(lsn3) == 0);
		System.out.println("Lsn = " + lsn);
		System.out.println("sizeof Lsn = " + lsn.getStoredLength());
	}

}
