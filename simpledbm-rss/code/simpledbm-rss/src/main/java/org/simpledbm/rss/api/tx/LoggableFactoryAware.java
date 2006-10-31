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
/*
 * LoggableFactoryAware.java
 *
 * Created on 21 November 2005, 23:38
 */

package org.simpledbm.rss.api.tx;

/**
 * A Loggable record that implements this interface will be injected with the
 * LoggableFactory responsible for generating Loggable objects. This is useful when
 * a Loggable object contains instances of other Loggable objects; it can use the
 * LoggableFactory instance to gain access to Loggable objects it needs.
 * 
 * @author Dibyendu Majumdar
 */
public interface LoggableFactoryAware {
	void setLoggableFactory(LoggableFactory factory);
}
