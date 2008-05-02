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
package org.simpledbm.rss.impl.event;

import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.simpledbm.rss.api.event.Event;
import org.simpledbm.rss.api.event.EventListener;
import org.simpledbm.rss.api.event.EventPublisher;

/**
 * A very simple implementation of EventPublisher.
 * 
 * @author dibyendumajumdar
 * @since 1 March 2008
 */
public class EventPublisherImpl implements EventPublisher {
	
	ReadWriteLock lock = new ReentrantReadWriteLock();
	Lock readLock = lock.readLock();
	Lock writeLock = lock.writeLock();
	
	ArrayList<EventListener> listeners = new ArrayList<EventListener>();

	public void addEventListener(EventListener listener) {
		writeLock.lock();
		try {
			if (!listeners.contains(listener)) {
				listeners.add(listener);
			}
		}
		finally {
			writeLock.unlock();
		}
	}

	public void publishEvent(Event event) {
		readLock.lock();
		try {
			for (EventListener listener: listeners) {
				try {
					listener.handleEvent(event);
				}
				catch (Throwable t) {
					//FIXME - report error
				}
			}
		}
		finally {
			readLock.unlock();
		}
	}
	
	public void removeEventListeners() {
		writeLock.lock();
		try {
			listeners.clear();
		}
		finally {
			writeLock.unlock();
		}
	}

	public void removeEventListener(EventListener listener) {
		writeLock.lock();
		try {
			listeners.remove(listener);
		}
		finally {
			writeLock.unlock();
		}
	}
}
