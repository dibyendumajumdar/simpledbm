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

import java.util.Iterator;

import org.simpledbm.common.util.Linkable;
import org.simpledbm.common.util.SimpleLinkedList;
import org.simpledbm.junit.BaseTestCase;

public class TestLinkedList extends BaseTestCase {

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
        for (Element e : ll) {
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
