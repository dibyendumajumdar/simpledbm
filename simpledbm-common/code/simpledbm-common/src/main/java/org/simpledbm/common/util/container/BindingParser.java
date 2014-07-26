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
package org.simpledbm.common.util.container;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;

import org.simpledbm.common.api.exception.SimpleDBMException;
import org.simpledbm.common.util.mcat.Message;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.common.util.mcat.MessageType;

public class BindingParser {

    static final Message m_ioError = new Message('P', 'C',
            MessageType.ERROR, 3, "IO Error occurred while reading from {0}");
    static final Message m_invalidLine = new Message('P', 'C',
            MessageType.ERROR, 4, "Syntax error at line {0} [{1}]");

    final ArrayList<Binding> bindings;

    public BindingParser(String fileName) {
        bindings = parseConfiguration(fileName);
    }

    public ArrayList<Binding> getBindings() {
        return bindings;
    }

    private ArrayList<Binding> parseConfiguration(String filename) {
        FileReader fileReader = null;
        LineNumberReader reader = null;
        ArrayList<Binding> list = new ArrayList<Binding>();
        try {
            fileReader = new FileReader(filename);
            reader = new LineNumberReader(new BufferedReader(fileReader));
            int ln = 0;
            String line = reader.readLine();
            while (null != line) {
                ln++;
                if (!isComment(line)) {
                    list.add(parseLine(ln, line));
                }
                line = reader.readLine();
            }
        } 
        catch (IOException e) {
            throw new SimpleDBMException(new MessageInstance(m_ioError, filename), e);
        }
        finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                }
            }
            if (fileReader != null) { 
                try {
                    fileReader.close();
                } catch (IOException e) {
                }
            }
        }
        return list;
    }

    private boolean isComment(String line) {
        return line.trim().startsWith("#") || line.trim().length() == 0;
    }

    private Binding parseLine(int ln, String line) {
        int equalAt = line.indexOf('=');
        if (equalAt == -1 || equalAt == 0 || equalAt == line.length() - 1) {
            throw new SimpleDBMException(new MessageInstance(m_invalidLine, ln, line));
        }
        String interfaceName = line.substring(0, equalAt).trim();
        String className = line.substring(equalAt + 1).trim();
        if (interfaceName.length() == 0 || className.length() == 0) {
            throw new SimpleDBMException(new MessageInstance(m_invalidLine, ln, line));
        }
        return new Binding(interfaceName, className);
    }
}
