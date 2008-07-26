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
 *    Email  : d dot majumdar at gmail dot com ignore
 */
package org.simpledbm.rss.util;

import org.simpledbm.junit.BaseTestCase;

public class TestSequencer extends BaseTestCase {

	public TestSequencer(String name) {
		super(name);
	}

	public void testSequencer() {
		
		WrappingSequencer sequencer = new WrappingSequencer(4);
		assertEquals(0, sequencer.getNext());
		assertEquals(1, sequencer.getNext());
		assertEquals(2, sequencer.getNext());
		assertEquals(3, sequencer.getNext());
		assertEquals(0, sequencer.getNext());
		assertEquals(1, sequencer.getNext());
		assertEquals(2, sequencer.getNext());
		assertEquals(3, sequencer.getNext());
		assertEquals(0, sequencer.getNext());
		assertEquals(1, sequencer.getNext());
		assertEquals(2, sequencer.getNext());
		assertEquals(3, sequencer.getNext());
		assertEquals(0, sequencer.getNext());

		/*
		WrappingSequencer seq = new WrappingSequencer(Integer.MAX_VALUE);
		seq.getAndSet(Integer.MAX_VALUE-3);
		System.err.println(seq.getNext());
		System.err.println(seq.getNext());
		System.err.println(seq.getNext());
		System.err.println(seq.getNext());
		System.err.println(seq.getNext());
		System.err.println(seq.getNext());
		System.err.println(seq.getNext());
		System.err.println(seq.getNext());
		System.err.println(seq.getNext());
		System.err.println(seq.getNext());

		AtomicInteger x = new AtomicInteger(Integer.MAX_VALUE);
		x.getAndSet(Integer.MAX_VALUE-3);
		System.err.println(x.getAndIncrement());
		System.err.println(x.getAndIncrement());
		System.err.println(x.getAndIncrement());
		System.err.println(x.getAndIncrement());
		System.err.println(x.getAndIncrement());
		System.err.println(x.getAndIncrement());
		System.err.println(x.getAndIncrement());
		System.err.println(x.getAndIncrement());
		System.err.println(x.getAndIncrement());
		System.err.println(x.getAndIncrement());
		System.err.println(x.getAndIncrement());
		System.err.println(x.getAndIncrement());
		System.err.println(x.getAndIncrement());
		System.err.println(x.getAndIncrement());
		System.err.println(x.getAndIncrement());
		System.err.println(x.getAndIncrement());
		System.err.println(x.getAndIncrement());
		*/
		
//		assertEquals((int)2147483644, sequencer.getNext());
//		assertEquals((int)2147482645, sequencer.getNext());
//		assertEquals((int)2147482646, sequencer.getNext());
//		assertEquals(0, sequencer.getNext());
//		assertEquals(1, sequencer.getNext());
//		assertEquals(2, sequencer.getNext());
//		assertEquals(3, sequencer.getNext());
	}
}
