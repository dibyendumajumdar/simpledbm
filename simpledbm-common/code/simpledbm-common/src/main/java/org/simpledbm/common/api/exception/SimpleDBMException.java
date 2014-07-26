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
