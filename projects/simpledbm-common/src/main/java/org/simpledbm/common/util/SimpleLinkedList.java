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

    /**
     * Add the supplied item to the end of the list.
     */
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

    /**
     * Add the supplied item to the beginning of the list.
     */
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

    /**
     * Insert the new item before the anchor item.
     * @param anchor The item before which the new item is to be added
     * @param link New item to be added
     */
    public final void insertBefore(E anchor, E link) {
        setOwner(link);
        if (anchor == null) {
            addFirst(link);
        } else {
            assert anchor.isMemberOf(this);
            setOwner(link);
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

    /**
     * Insert the new item after the anchor item.
     * @param anchor The item after which the new item is to be added
     * @param link New item to be added
     */
    public final void insertAfter(E anchor, E link) {
        if (anchor == null) {
            addLast(link);
        } else {
            assert anchor.isMemberOf(this);
            setOwner(link);
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

    private final void removeInternal(Linkable link) {
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

    /**
     * Remove the supplied item.
     */
    public final void remove(E e) {
        removeInternal(e);
    }

    /**
     * Search for the supplied item.
     * @param link Item to be searched for
     * @return true if item was found, false otherwise
     */
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

    /**
     * Clear the linked list.
     */
    public final void clear() {
        Linkable cursor = head;
        while (cursor != null) {
            Linkable next = cursor.getNext();
            cursor.setNext(null);
            cursor.setPrev(null);
            cursor.setOwner(null);
            cursor = next;
        }
        count = 0;
        head = null;
        tail = null;
    }

    /**
     * Get the first item in the list.
     */
    @SuppressWarnings("unchecked")
    public final E getFirst() {
        return (E) head;
    }

    /**
     * Get the last item in the list.
     */
    @SuppressWarnings("unchecked")
    public final E getLast() {
        return (E) tail;
    }

    /**
     * Get the next item in the list.
     */
    @SuppressWarnings("unchecked")
    public final E getNext(E cursor) {
        return (E) cursor.getNext();
    }

    /**
     * Get the number of items in the list
     */
    public final int size() {
        return count;
    }

    /**
     * Check if the list is empty, i.e., has no items.
     */
    public final boolean isEmpty() {
        return count == 0;
    }

    /**
     * Same as {@link #addLast(Linkable)}
     */
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
