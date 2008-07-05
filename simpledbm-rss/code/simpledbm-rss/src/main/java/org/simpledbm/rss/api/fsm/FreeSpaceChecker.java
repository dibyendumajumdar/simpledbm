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
package org.simpledbm.rss.api.fsm;

/**
 * Defines the interface for the callback that will be
 * used to test whether a page meets the space requirements.
 * Using a callback enables the space manager to delegate this
 * decision to the client.
 * 
 * @author Dibyendu Majumdar
 * @since 07-Sep-2005
 */
public interface FreeSpaceChecker {

    /**
     * Checks whether the space in the page meets the requirements of the
     * caller.
     * 
     * @param value The current space value
     * @return true if the page would meet the requirements.
     */
    boolean hasSpace(int value);
}
