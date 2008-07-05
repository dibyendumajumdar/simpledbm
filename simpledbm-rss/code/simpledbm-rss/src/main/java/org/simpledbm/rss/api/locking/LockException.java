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
/*
 * Created on: Jul 26, 2005
 * Author: Dibyendu Majumdar
 */
package org.simpledbm.rss.api.locking;

import org.simpledbm.rss.api.exception.RSSException;

/**
 * LockException is the base class for all exceptions raised by the Locking sub-system.
 * @author Dibyendu Majumdar
 * @since 27-July-2005
 */
public class LockException extends RSSException {

    private static final long serialVersionUID = 1L;

    public LockException() {
        super();
    }

    public LockException(String arg0) {
        super(arg0);
    }

    public LockException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    public LockException(Throwable arg0) {
        super(arg0);
    }
}
