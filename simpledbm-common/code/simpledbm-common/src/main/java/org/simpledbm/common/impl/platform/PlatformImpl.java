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
package org.simpledbm.common.impl.platform;

import java.util.HashMap;
import java.util.Properties;

import org.simpledbm.common.api.event.EventPublisher;
import org.simpledbm.common.api.exception.ExceptionHandler;
import org.simpledbm.common.api.exception.ExceptionHandlerFactory;
import org.simpledbm.common.api.exception.ExceptionHandlerFactoryImpl;
import org.simpledbm.common.api.info.InformationManager;
import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.api.thread.Scheduler;
import org.simpledbm.common.impl.event.EventPublisherImpl;
import org.simpledbm.common.impl.info.InformationManagerImpl;
import org.simpledbm.common.impl.thread.SimpleScheduler;
import org.simpledbm.common.tools.diagnostics.TraceBuffer;
import org.simpledbm.common.util.ClassUtils;
import org.simpledbm.common.util.container.ConfigProperties;
import org.simpledbm.common.util.container.ConfigPropertiesImpl;
import org.simpledbm.common.util.logging.JDK4LoggerFactory;
import org.simpledbm.common.util.logging.Logger;
import org.simpledbm.common.util.logging.LoggerFactory;

public class PlatformImpl implements Platform {

    final TraceBuffer traceBuffer = new TraceBuffer(false);
    final HashMap<String, PlatformObjects> pomap = new HashMap<String, PlatformObjects>();
    final InformationManager infoManager = new InformationManagerImpl();
    final EventPublisher eventPublisher = new EventPublisherImpl();
    final ExceptionHandlerFactory exceptionHandlerFactory;
    final LoggerFactory loggerFactory;
    final Scheduler scheduler;

    public PlatformImpl(Properties props) {
        ConfigProperties properties = new ConfigPropertiesImpl(props);
        loggerFactory = new JDK4LoggerFactory(properties);
        exceptionHandlerFactory = new ExceptionHandlerFactoryImpl(loggerFactory);
        this.scheduler = new SimpleScheduler(props);
    }

    ExceptionHandler getExceptionHandler(String loggerName) {
        return exceptionHandlerFactory.getExceptionHandler(loggerName);
    }

    Logger getLogger(String loggerName) {
        return loggerFactory.getLogger(loggerName);
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
            this.exceptionHandler = platform.getExceptionHandler(loggerName);
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
