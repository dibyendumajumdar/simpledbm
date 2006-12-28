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
package org.simpledbm.rss.api.wal;

import org.simpledbm.rss.api.exception.RSSException;

/**
 * Base class for all exceptions thrown by the Log implementations.
 * 
 * @author Dibyendu Majumdar
 * @since 10-Jun-2005
 */
public class LogException extends RSSException {

    private static final long serialVersionUID = 1L;

    public LogException() {
        super();
    }

    public LogException(String arg0) {
        super(arg0);
    }

    public LogException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    public LogException(Throwable arg0) {
        super(arg0);
    }
}
