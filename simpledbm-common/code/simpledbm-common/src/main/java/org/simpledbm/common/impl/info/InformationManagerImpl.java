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

    public static void main(String args[]) {
        InformationManager im = new InformationManagerImpl();
        LongStatistic ls1 = im
                .newLongStatistic("org.simpledbm.common.info.Test");
        ls1.increment();
        LongStatistic ls2 = im
                .newLongStatistic("org.simpledbm.common.info.Test2");
        ls2.increment();
        InfoStatistic is = im.newInfoStatistic("test");
        is.set("value");
        im.printStatistics(System.out);
    }

    public InfoStatistic newInfoStatistic(String name) {
        return newStatistic(name, new InfoStatisticImpl(name));
    }

}
