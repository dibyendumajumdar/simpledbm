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
package org.simpledbm.rss.api.event;

/**
 * An EventPublisher is responsible for pushing events out to all registered
 * event listeners.
 * 
 * @author dibyendumajumdar
 * @since 1 March 2008
 */
public interface EventPublisher {

	/**
	 * Adds specified EventListener to the list of registered event listeners.
	 */
	void addEventListener(EventListener listener);

	/**
	 * Removes specified EventListener from the list of registered event listeners.
	 */
	void removeEventListener(EventListener listener);
	
	/**
	 * Clears all the EventListeners.
	 */
	void removeEventListeners();
	
	/**
	 * Publishes an event to all the registered EventListeners. Note that the
	 * event will be broadcast to all listeners;  it is the responsibility of the listener
	 * to decide which events are of interest.
	 */
	void publishEvent(Event event);

}
