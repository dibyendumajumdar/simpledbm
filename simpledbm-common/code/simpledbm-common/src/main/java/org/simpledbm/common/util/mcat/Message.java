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

import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.util.StorableString;
import org.simpledbm.common.util.TypeSize;

/**
 * A message holds together a number of items related to a single message.
 * 
 * @author dibyendumajumdar
 */
public class Message implements Storable {
    final int code;
    final MessageType type;
    final char subsystem;
    final char module;
    final String text;
    final String formattedText;

    volatile boolean cached = false;
    StorableString storableText;

    static final String prefix = "00000";

    private String pad(int i) {
        String s = prefix + Integer.toString(i);
        return s.substring(s.length() - prefix.length(), s.length());
    }

    /**
     * A Message holds together a number of items that relate to the same
     * message.
     * 
     * @param module A single character module identifier
     * @param type Message type - such as 'W' for warn, 'I' for info, etc.
     * @param errorCode A numeric error code, must be unique
     * @param text The text that goes with the message, may contain argument
     *            placeholders
     */
    public Message(char subsystem, char module, MessageType type,
            int errorCode, String text) {
        super();
        this.subsystem = subsystem;
        this.module = module;
        this.type = type;
        this.code = errorCode;
        this.text = text;
        formattedText = "SIMPLEDBM-" + type.toText() + subsystem + module
                + pad(code) + ": " + text;
    }

    public Message(ByteBuffer bb) {
        code = bb.getInt();
        type = MessageType.fromText(bb.getChar());
        subsystem = bb.getChar();
        module = bb.getChar();
        storableText = new StorableString(bb);
        text = storableText.toString();
        formattedText = "SIMPLEDBM-" + type.toText() + subsystem + module
                + pad(code) + ": " + text;
        cached = true;
    }

    public String getKey() {
        return "" + type.toText() + subsystem + module + pad(code);
    }

    /**
     * Creates the formatted/storable version
     */
    void inflate() {
        if (cached)
            return;
        synchronized (this) {
            if (storableText == null) {
                storableText = new StorableString(text);
            }
            cached = true;
        }
    }

    public String toString() {
        return formattedText;
    }

    public int getCode() {
        return code;
    }

    public MessageType getType() {
        return type;
    }

    public int getStoredLength() {
        inflate();
        return TypeSize.INTEGER + TypeSize.CHARACTER * 3
                + storableText.getStoredLength();
    }

    public void store(ByteBuffer bb) {
        inflate();
        bb.putInt(code);
        bb.putChar(type.toText());
        bb.putChar(subsystem);
        bb.putChar(module);
        storableText.store(bb);
    }
}
