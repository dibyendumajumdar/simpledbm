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
package org.simpledbm.rss.api.tx;

import java.nio.ByteBuffer;

import org.simpledbm.rss.api.wal.LogRecord;

/**
 * Defines the factory interface for creating {@link Loggable} objects of appropriate types.
 * 
 * @author Dibyendu Majumdar
 * @since 23-Aug-2005
 */
public interface LoggableFactory {

    /**
     * Instantiate a Loggable object using type information stored in the
     * ByteBuffer. The typecode must be present in the first two bytes (as a short)
     * of the buffer.
     */
    public Loggable getInstance(ByteBuffer bb);

    /**
     * Create a new Loggable object of the specified type. The Loggable
     * object's module id field will be set to the specified module id.
     */
    public Loggable getInstance(int moduleId, int typecode);

    /**
     * Create an instance of Loggable object from the raw log data.
     * The first two bytes in the data must contain the typecode
     * of the Loggable object.
     */
    public Loggable getInstance(LogRecord logRec);

}
