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
package org.simpledbm.common.util;

import java.util.Iterator;

/**
 * This implementation of LinkedList that is optimized for element removal. The
 * standard Java linked list implementation is non-intrusive and inefficient for
 * element removals. This implementation requires elements to extend the
 * {@link Linkable} abstract class.
 * <p>
 * The implementation is not thread-safe. Caller must ensure thread safety.
 * 
 * @author Dibyendu Majumdar
 */
public class SimpleLinkedList<E extends Linkable> implements Iterable<E> {

    Linkable head;

    Linkable tail;

    /**
     * Tracks the number of members in the list.
     */
    int count;

    private void setOwner(E link) {
        assert !link.isMemberOf(this);
        link.setOwner(this);
    }

    private void removeOwner(Linkable link) {
        assert link.isMemberOf(this);
        link.setOwner(null);
    }

    public final void addLast(E link) {
        setOwner(link);
        if (head == null)
            head = link;
        link.setPrev(tail);
        if (tail != null)
            tail.setNext(link);
        tail = link;
        link.setNext(null);
        count++;
    }

    public final void addFirst(E link) {
        setOwner(link);
        if (tail == null)
            tail = link;
        link.setNext(head);
        if (head != null)
            head.setPrev(link);
        head = link;
        link.setPrev(null);
        count++;
    }

    public final void insertBefore(E anchor, E link) {
        setOwner(link);
        if (anchor == null) {
            addFirst(link);
        } else {
            Linkable prev = anchor.getPrev();
            link.setNext(anchor);
            link.setPrev(prev);
            anchor.setPrev(link);
            if (prev == null) {
                head = link;
            } else {
                prev.setNext(link);
            }
            count++;
        }
    }

    public final void insertAfter(E anchor, E link) {
        setOwner(link);
        if (anchor == null) {
            addLast(link);
        } else {
            Linkable next = anchor.getNext();
            link.setPrev(anchor);
            link.setNext(next);
            anchor.setNext(link);
            if (next == null) {
                tail = link;
            } else {
                next.setPrev(link);
            }
            count++;
        }
    }

    private void removeInternal(Linkable link) {
        removeOwner(link);
        Linkable next = link.getNext();
        Linkable prev = link.getPrev();
        if (next != null) {
            next.setPrev(prev);
        } else {
            tail = prev;
        }
        if (prev != null) {
            prev.setNext(next);
        } else {
            head = next;
        }
        link.setNext(null);
        link.setPrev(null);
        count--;
    }

    public final void remove(E e) {
        removeInternal(e);
    }

    public final boolean contains(E link) {
        Linkable cursor = head;

        while (cursor != null) {
            if (cursor == link || cursor.equals(link)) {
                return true;
            } else {
                cursor = cursor.getNext();
            }
        }
        return false;
    }

    public final void clear() {
        while (count > 0)
            removeInternal(head);
    }

    @SuppressWarnings("unchecked")
    public final E getFirst() {
        return (E) head;
    }

    @SuppressWarnings("unchecked")
    public final E getLast() {
        return (E) tail;
    }

    @SuppressWarnings("unchecked")
    public final E getNext(E cursor) {
        return (E) cursor.getNext();
    }

    public final int size() {
        return count;
    }

    public final boolean isEmpty() {
        return count == 0;
    }

    public final void push(E link) {
        addLast(link);
    }

    @SuppressWarnings("unchecked")
    public final E pop() {
        Linkable popped = tail;
        if (popped != null) {
            removeInternal(popped);
        }
        return (E) popped;
    }

    public Iterator<E> iterator() {
        return new Iter<E>(this);
    }

    static final class Iter<E extends Linkable> implements Iterator<E> {

        final SimpleLinkedList<E> ll;

        E nextE;

        E currentE;

        Iter(SimpleLinkedList<E> ll) {
            this.ll = ll;
            nextE = ll.getFirst();
        }

        public boolean hasNext() {
            return nextE != null;
        }

        @SuppressWarnings("unchecked")
        public E next() {
            currentE = nextE;
            if (nextE != null) {
                nextE = (E) nextE.getNext();
            }
            return currentE;
        }

        public void remove() {
            if (currentE != null) {
                ll.remove(currentE);
                currentE = null;
            }
        }
    }
}
