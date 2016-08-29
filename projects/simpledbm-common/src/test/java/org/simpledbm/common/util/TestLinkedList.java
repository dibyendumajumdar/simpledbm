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
package org.simpledbm.common.util;

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
