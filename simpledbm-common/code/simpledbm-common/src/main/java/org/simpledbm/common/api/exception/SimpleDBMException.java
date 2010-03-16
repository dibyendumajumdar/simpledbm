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
 *    Linking this library statically or dynamically with other modules 
 *    is making a combined work based on this library. Thus, the terms and
 *    conditions of the GNU General Public License cover the whole
 *    combination.
 *    
 *    As a special exception, the copyright holders of this library give 
 *    you permission to link this library with independent modules to 
 *    produce an executable, regardless of the license terms of these 
 *    independent modules, and to copy and distribute the resulting 
 *    executable under terms of your choice, provided that you also meet, 
 *    for each linked independent module, the terms and conditions of the 
 *    license of that module.  An independent module is a module which 
 *    is not derived from or based on this library.  If you modify this 
 *    library, you may extend this exception to your version of the 
 *    library, but you are not obligated to do so.  If you do not wish 
 *    to do so, delete this exception statement from your version.
 *
 *    Project: www.simpledbm.org
 *    Author : Dibyendu Majumdar
 *    Email  : d dot majumdar at gmail dot com ignore
 */
package org.simpledbm.common.api.exception;

import java.nio.ByteBuffer;

import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.common.util.mcat.MessageType;

/**
 * Base exception class for SimpleDBM exceptions. SimpleDBMException class
 * implements Storable so that the exception can be transferred over the
 * network; however, stack traces and underlying exceptions are not included in
 * the serialized format.
 * 
 * @author Dibyendu Majumdar
 * @since 27 Dec 2006
 */
public class SimpleDBMException extends RuntimeException implements Storable {

    private static final long serialVersionUID = 1L;

    final MessageInstance m;

    public SimpleDBMException(MessageInstance m, Throwable arg1) {
        super(m.toString(), arg1);
        this.m = m;
    }

    public SimpleDBMException(MessageInstance m) {
        super(m.toString());
        this.m = m;
    }

    public SimpleDBMException(ByteBuffer bb) {
        super();
        m = new MessageInstance(bb);
    }

    @Override
    public String getMessage() {
        return m.toString();
    }

    @Override
    public String toString() {
        return getClass().getName() + ": " + getMessage();
    }

    public String getMessageKey() {
        return m.getKey();
    }

    public int getStoredLength() {
        return m.getStoredLength();
    }

    public void store(ByteBuffer bb) {
        m.store(bb);
    }

    public int getErrorCode() {
        return m.getCode();
    }

    public MessageType getType() {
        return m.getType();
    }
}
