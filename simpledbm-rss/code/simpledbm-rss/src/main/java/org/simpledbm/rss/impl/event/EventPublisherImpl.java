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
package org.simpledbm.rss.impl.event;

import java.util.ArrayList;
import java.util.Properties;
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

	public EventPublisherImpl(Properties p) {
		
	}
	
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
