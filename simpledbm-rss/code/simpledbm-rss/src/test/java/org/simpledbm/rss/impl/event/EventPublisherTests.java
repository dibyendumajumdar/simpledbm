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
package org.simpledbm.rss.impl.event;

import org.simpledbm.junit.BaseTestCase;
import org.simpledbm.rss.api.event.Event;
import org.simpledbm.rss.api.event.EventListener;
import org.simpledbm.rss.api.event.EventPublisher;
import org.simpledbm.rss.api.event.common.ShutdownEvent;

public class EventPublisherTests extends BaseTestCase {

	public EventPublisherTests() {
	}

	public EventPublisherTests(String arg0) {
		super(arg0);
	}

	public void testBasicFunctions() {
		EventPublisher ep = new EventPublisherImpl();
		MyListener el = new MyListener();
		MyListener el2 = new MyListener();
		
		ep.addEventListener(el);
		ep.addEventListener(el2);
		ep.publishEvent(new ShutdownEvent());
		assertTrue(el.isEventReceived());
		assertTrue(el2.isEventReceived());
	}

	
	static class MyListener implements EventListener {

		boolean eventReceived = false;
		
		public Object handleEvent(Event event) {
			if (event != null && event instanceof ShutdownEvent) {
				eventReceived = true;
			}
			return null;
		}
		
		boolean isEventReceived() {
			return eventReceived;
		}
		
	}
	
}
