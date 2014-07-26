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
package org.simpledbm.common.impl.info;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.HashMap;

import org.simpledbm.common.api.exception.SimpleDBMException;
import org.simpledbm.common.api.info.InfoStatistic;
import org.simpledbm.common.api.info.InformationManager;
import org.simpledbm.common.api.info.LongStatistic;
import org.simpledbm.common.api.info.Statistic;
import org.simpledbm.common.util.Dumpable;
import org.simpledbm.common.util.mcat.Message;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.common.util.mcat.MessageType;

public class InformationManagerImpl implements InformationManager {

    final HashMap<String, Statistic> map = new HashMap<String, Statistic>();

    static final Message alreadyExists = new Message('C', 'I',
            MessageType.ERROR, 1,
            "A statistic already exists with the name {0} and type {1}");

    public Statistic getStatistic(String name) {
        synchronized (map) {
            return map.get(name);
        }
    }

    @SuppressWarnings("unchecked")
	private <T extends Statistic> T newStatistic(String name, T statistic) {
        synchronized (map) {
            Statistic s = map.get(name);
            if (s != null) {
                if (s.getClass() != statistic.getClass()) {
                    throw new SimpleDBMException(new MessageInstance(alreadyExists,
                            name, s.getClass().getName()));
                }
                return (T) s;
            }
            map.put(name, statistic);
            return statistic;
        }
    }

    public LongStatistic newLongStatistic(String name) {
        return newStatistic(name, new LongStatisticImpl(name));
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        synchronized (map) {
            Collection<Statistic> entries = map.values();
            for (Statistic s : entries) {
                s.appendTo(sb);
                sb.append(Dumpable.newline);
            }
        }
        return sb.toString();
    }

    public void printStatistics(OutputStream stream) {;
        try {
            stream.write(toString().getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
        } catch (IOException e) {
        }
    }

    public InfoStatistic newInfoStatistic(String name) {
        return newStatistic(name, new InfoStatisticImpl(name));
    }

}
