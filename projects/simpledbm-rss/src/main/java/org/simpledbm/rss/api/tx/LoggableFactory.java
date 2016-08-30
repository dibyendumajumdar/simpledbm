/**
 * DO NOT REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Contributor(s):
 *
 * The Original Software is SimpleDBM (www.simpledbm.org).
 * The Initial Developer of the Original Software is Dibyendu Majumdar.
 *
 * Portions Copyright 2005-2014 Dibyendu Majumdar. All Rights Reserved.
 *
 * The contents of this file are subject to the terms of the
 * Apache License Version 2 (the "APL"). You may not use this
 * file except in compliance with the License. A copy of the
 * APL may be obtained from:
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the APL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the APL, the GPL or the LGPL.
 *
 * Copies of GPL and LGPL may be obtained from:
 * http://www.gnu.org/licenses/license-list.html
 */
package org.simpledbm.rss.api.tx;

import java.nio.ByteBuffer;

import org.simpledbm.rss.api.wal.LogRecord;

/**
 * Defines the factory interface for creating {@link Loggable} objects of
 * appropriate types.
 * 
 * @author Dibyendu Majumdar
 * @since 23-Aug-2005
 */
public interface LoggableFactory {

    /**
     * Instantiate a Loggable object using type information stored in the
     * ByteBuffer. The typecode must be present in the first two bytes (as a
     * short) of the buffer.
     */
    public Loggable getInstance(ByteBuffer bb);

    //    /**
    //     * Create a new Loggable object of the specified type. The Loggable
    //     * object's module id field will be set to the specified module id.
    //     */
    //    public Loggable getInstance(int moduleId, int typecode);

    /**
     * Create an instance of Loggable object from the raw log data. The first
     * two bytes in the data must contain the typecode of the Loggable object.
     */
    public Loggable getInstance(LogRecord logRec);

}
