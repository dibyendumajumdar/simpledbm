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
package org.simpledbm.common.util.mcat;

import java.nio.ByteBuffer;
import java.text.MessageFormat;

import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.util.StorableString;
import org.simpledbm.common.util.TypeSize;

/**
 * MessageInstance holds a message and its arguments.
 * 
 * @author dibyendumajumdar
 */
public class MessageInstance implements Storable {

    final Message m;
    final Object[] args;
    final String formattedText;

    volatile boolean cached = false;
    StorableString storableText;
    StorableString[] storableArgs;

    public MessageInstance(Message m, Object... args) {
        this.m = m;
        this.args = args;
        if (args != null) {
            this.formattedText = MessageFormat.format(m.toString(), args);
        } else {
            this.formattedText = m.toString();
        }
    }

    public MessageInstance(Message m) {
        super();
        this.m = m;
        this.args = null;
        this.formattedText = m.toString();
    }

    public MessageInstance(ByteBuffer bb) {
        this.m = new Message(bb);
        short n = bb.getShort();
        if (n != 0) {
            storableArgs = new StorableString[n];
            for (int i = 0; i < n; i++) {
                storableArgs[i] = new StorableString(bb);
            }
            args = new String[n];
            for (int i = 0; i < n; i++) {
                args[i] = storableArgs[i].toString();
            }
        } else {
            args = null;
        }
        if (args != null) {
            this.formattedText = MessageFormat.format(m.toString(), args);
        } else {
            this.formattedText = m.toString();
        }
    }

    public int getCode() {
        return m.getCode();
    }

    public MessageType getType() {
        return m.getType();
    }

    public String getKey() {
        return m.getKey();
    }

    private void deflate() {
        if (cached)
            return;
        synchronized (this) {
            if (storableText == null) {
                storableText = new StorableString(formattedText);
            }
            if (storableArgs == null) {
                storableArgs = new StorableString[args.length];
                for (int i = 0; i < args.length; i++) {
                    storableArgs[i] = new StorableString(args[i].toString());
                }
            }
            cached = true;
        }
    }

    @Override
    public String toString() {
        return formattedText;
    }

    public int getStoredLength() {
        deflate();
        int n = m.getStoredLength();
        n += TypeSize.SHORT;
        if (storableArgs != null) {
            for (int i = 0; i < storableArgs.length; i++) {
                n += storableArgs[i].getStoredLength();
            }
        }
        return n;
    }

    public void store(ByteBuffer bb) {
        deflate();
        m.store(bb);
        if (args == null) {
            bb.putShort((short) 0);
        } else {
            bb.putShort((short) args.length);
            for (int i = 0; i < storableArgs.length; i++) {
                storableArgs[i].store(bb);
            }
        }
    }
}
