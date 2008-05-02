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
