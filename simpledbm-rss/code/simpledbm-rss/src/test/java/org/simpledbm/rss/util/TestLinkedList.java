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
		
	}
	
	public void testBasics() {
		LinkedList<Element> ll = new LinkedList<Element>();
		for (int i = 0; i < 10; i++) {
			ll.append(new Element(i));
		}
		assertEquals(10, ll.size());
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
			ll.prepend(new Element(i));
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
