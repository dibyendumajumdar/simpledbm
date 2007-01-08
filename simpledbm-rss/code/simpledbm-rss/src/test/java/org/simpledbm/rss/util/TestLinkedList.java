package org.simpledbm.rss.util;

import java.util.Iterator;

import junit.framework.TestCase;

public class TestLinkedList extends TestCase {
	
	static final class Element extends Linkable {
		int i;
		
		public Element(int i) {
			this.i = i;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final Element other = (Element) obj;
			if (i != other.i)
				return false;
			return true;
		}

		public String toString() {
			return "E(" + i + ")";
		}
		
	}
	
	public void testBasics() {
		SimpleLinkedList<Element> ll = new SimpleLinkedList<Element>();
		for (int i = 0; i < 10; i++) {
			ll.addLast(new Element(i));
		}
		assertEquals(10, ll.size());
		for (Element e: ll) {
			System.out.println(e);
		}
		Iterator<Element> iter = ll.iterator();
		int x = 0;
		while (iter.hasNext()) {
			Element e = iter.next();
			assertEquals(x, e.i);
			iter.remove();
			x++;
		}
		assertEquals(0, ll.size());
		for (int i = 0; i < 10; i++) {
			ll.addFirst(new Element(i));
		}
		assertEquals(10, ll.size());
		x = 9;
		while (iter.hasNext()) {
			Element e = iter.next();
			assertEquals(x, e.i);
			iter.remove();
			x--;
		}
	}

}
