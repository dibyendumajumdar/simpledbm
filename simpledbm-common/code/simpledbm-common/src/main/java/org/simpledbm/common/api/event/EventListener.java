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
package org.simpledbm.common.api.event;

/**
 * EventListener interface must be implemented by modules that wish to be
 * notified of events.
 * 
 * @author dibyendumajumdar
 * @since 1 March 2008
 */
public interface EventListener {

	/**
	 * Handles the event. Must check the event type and only react if the event
	 * is of interest. Note that this method must not perform any time-consuming 
	 * activity or acquire resources. It must be as light-weight/quick as possible.
	 */
	Object handleEvent(Event event);
	
}
