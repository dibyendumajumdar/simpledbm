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
        super();
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
