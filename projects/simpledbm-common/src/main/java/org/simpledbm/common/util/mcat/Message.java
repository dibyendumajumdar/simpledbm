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

import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.util.StorableString;
import org.simpledbm.common.util.TypeSize;

/**
 * A message holds together a number of items related to a single message.
 * 
 * @author dibyendumajumdar
 */
public final class Message implements Storable {
    final int code;
    final MessageType type;
    final char subsystem;
    final char module;
    final String text;
    final String formattedText;

    final StorableString storableText;

    static final String prefix = "00000";

    private final String pad(int i) {
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
        storableText = new StorableString(text);
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
    }

    public final String getKey() {
        return "" + type.toText() + subsystem + module + pad(code);
    }

    public final String toString() {
        return formattedText;
    }

    public final int getCode() {
        return code;
    }

    public final MessageType getType() {
        return type;
    }

    public final int getStoredLength() {
        return TypeSize.INTEGER + TypeSize.CHARACTER * 3
                + storableText.getStoredLength();
    }

    public final void store(ByteBuffer bb) {
        bb.putInt(code);
        bb.putChar(type.toText());
        bb.putChar(subsystem);
        bb.putChar(module);
        storableText.store(bb);
    }
}
