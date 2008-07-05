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

import org.simpledbm.rss.api.exception.RSSException;

/**
 * Exception class for the Space Manager module.
 */
public class FreeSpaceManagerException extends RSSException {

    public static class TestException extends FreeSpaceManagerException {

        private static final long serialVersionUID = 1L;

    }

    private static final long serialVersionUID = 5065727917034813269L;

    public FreeSpaceManagerException() {
        super();
    }

    public FreeSpaceManagerException(String message) {
        super(message);
    }

    public FreeSpaceManagerException(String message, Throwable cause) {
        super(message, cause);
    }

    public FreeSpaceManagerException(Throwable cause) {
        super(cause);
    }

}
