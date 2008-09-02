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
 *    Project: www.simpledbm.org
 *    Author : Dibyendu Majumdar
 *    Email  : d dot majumdar at gmail dot com ignore
 */
package org.simpledbm.rss.impl.tx;

import java.nio.ByteBuffer;
import java.util.Properties;

import org.simpledbm.rss.api.registry.ObjectRegistry;
import org.simpledbm.rss.api.tx.BaseLoggable;
import org.simpledbm.rss.api.tx.Loggable;
import org.simpledbm.rss.api.tx.LoggableFactory;
import org.simpledbm.rss.api.wal.LogRecord;

/**
 * A factory for creating {@link org.simpledbm.rss.api.tx.Loggable} objects.
 * 
 * @author Dibyendu Majumdar
 * @since 23-Aug-2005
 */
public final class LoggableFactoryImpl implements LoggableFactory {

    private final ObjectRegistry objectFactory;

    public LoggableFactoryImpl(ObjectRegistry objectFactory, Properties p) {
        this.objectFactory = objectFactory;
    }

    public final Loggable getInstance(ByteBuffer bb) {
    	return (Loggable) objectFactory.getInstance(bb);
//        bb.mark();
//        short typecode = bb.getShort();
//        bb.reset();
//        BaseLoggable loggable = (BaseLoggable) objectFactory
//            .getInstance(typecode);
//        if (loggable instanceof LoggableFactoryAware) {
//            ((LoggableFactoryAware) loggable).setLoggableFactory(this);
//        }
//        loggable.init();
//        loggable.retrieve(bb);
//        return loggable;
    }

    public final Loggable getInstance(LogRecord logRec) {
        byte[] data = logRec.getData();
        ByteBuffer bb = ByteBuffer.wrap(data);
        Loggable loggable = getInstance(bb);
        loggable.setLsn(logRec.getLsn());
        // TODO loggable.setPrevLsn();
        return loggable;
    }

//    public final Loggable getInstance(int moduleId, int typecode) {
//        BaseLoggable loggable = (BaseLoggable) objectFactory
//            .getInstance(typecode);
//        loggable.setTypecode(typecode);
//        loggable.setModuleId(moduleId);
////        if (loggable instanceof LoggableFactoryAware) {
////            ((LoggableFactoryAware) loggable).setLoggableFactory(this);
////        }
////        loggable.init();
//        return loggable;
//    }

}
