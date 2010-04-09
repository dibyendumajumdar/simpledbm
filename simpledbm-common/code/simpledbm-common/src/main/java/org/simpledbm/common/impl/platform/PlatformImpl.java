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
package org.simpledbm.common.impl.platform;

import java.util.HashMap;
import java.util.Properties;

import org.simpledbm.common.api.event.EventPublisher;
import org.simpledbm.common.api.exception.ExceptionHandler;
import org.simpledbm.common.api.info.InformationManager;
import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.api.thread.Scheduler;
import org.simpledbm.common.impl.event.EventPublisherImpl;
import org.simpledbm.common.impl.info.InformationManagerImpl;
import org.simpledbm.common.impl.thread.SimpleScheduler;
import org.simpledbm.common.tools.diagnostics.TraceBuffer;
import org.simpledbm.common.util.ClassUtils;
import org.simpledbm.common.util.logging.Logger;

public class PlatformImpl implements Platform {

    final TraceBuffer traceBuffer = new TraceBuffer();
    final HashMap<String, PlatformObjects> pomap = new HashMap<String, PlatformObjects>();
    final InformationManager infoManager = new InformationManagerImpl();
    final EventPublisher eventPublisher = new EventPublisherImpl();
    final Scheduler scheduler;

    public PlatformImpl(Properties props) {
        Logger.configure(props);
        this.scheduler = new SimpleScheduler(props);
    }

    ExceptionHandler getExceptionHandler(Logger log) {
        return ExceptionHandler.getExceptionHandler(log);
    }

    Logger getLogger(String loggerName) {
        return Logger.getLogger(loggerName);
    }

    public TraceBuffer getTraceBuffer() {
        return traceBuffer;
    }

    public InformationManager getInfoManager() {
        return infoManager;
    }

    public EventPublisher getEventPublisher() {
        return eventPublisher;
    }

    public PlatformObjects getPlatformObjects(String loggerName) {
        PlatformObjects po = null;
        synchronized (pomap) {
            po = pomap.get(loggerName);
            if (po != null) {
                return po;
            }
            po = new PlatformObjectsImpl(this, loggerName);
            pomap.put(loggerName, po);
        }
        return po;
    }

    static final class PlatformObjectsImpl implements PlatformObjects {

        final PlatformImpl platform;
        final Logger log;
        final ExceptionHandler exceptionHandler;
        final ClassUtils classUtils;

        PlatformObjectsImpl(PlatformImpl platform, String loggerName) {
            this.platform = platform;
            this.log = platform.getLogger(loggerName);
            this.exceptionHandler = platform.getExceptionHandler(log);
            this.classUtils = new ClassUtils();
        }

        public final ExceptionHandler getExceptionHandler() {
            return exceptionHandler;
        }

        public final Logger getLogger() {
            return log;
        }

        public ClassUtils getClassUtils() {
            return classUtils;
        }

        public TraceBuffer getTraceBuffer() {
            return platform.getTraceBuffer();
        }

        public EventPublisher getEventPublisher() {
            return platform.eventPublisher;
        }

        public InformationManager getInformationManager() {
            return platform.infoManager;
        }

        public Platform getPlatform() {
            return platform;
        }
    }

    public void shutdown() {
        scheduler.shutdown();
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

}
