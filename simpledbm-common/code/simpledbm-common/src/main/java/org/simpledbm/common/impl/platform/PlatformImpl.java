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
package org.simpledbm.common.impl.platform;

import java.util.Properties;

import org.simpledbm.common.api.exception.ExceptionHandler;
import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.tools.diagnostics.TraceBuffer;
import org.simpledbm.common.util.ClassUtils;
import org.simpledbm.common.util.logging.Logger;
import org.simpledbm.common.util.mcat.MessageCatalog;

public class PlatformImpl implements Platform {
	
	final TraceBuffer traceBuffer = new TraceBuffer();

	public PlatformImpl(Properties props) {
        Logger.configure(props);		
	}
	
	ExceptionHandler getExceptionHandler(Logger log) {
		return ExceptionHandler.getExceptionHandler(log);
	}

	Logger getLogger(String loggerName) {
		return Logger.getLogger(loggerName);
	}

	MessageCatalog getMessageCatalog() {
		return MessageCatalog.getMessageCatalog();
	}

	public TraceBuffer getTraceBuffer() {
		return traceBuffer;
	}
	
	public PlatformObjects getPlatformObjects(String loggerName) {
		Logger log = getLogger(loggerName);
		ExceptionHandler exceptionHandler = getExceptionHandler(log);
		MessageCatalog messageCatalog = getMessageCatalog();
		ClassUtils classUtils = new ClassUtils();
		return new PlatformObjectsImpl(traceBuffer, log, exceptionHandler, messageCatalog, classUtils);
	}
	
	static final class PlatformObjectsImpl implements PlatformObjects {
		
		final Logger log;
		final ExceptionHandler exceptionHandler;
		final MessageCatalog messageCatalog;
		final ClassUtils classUtils;
		final TraceBuffer traceBuffer;

		PlatformObjectsImpl(TraceBuffer traceBuffer, Logger log, ExceptionHandler exceptionHandler, MessageCatalog messageCatalog, ClassUtils classUtils) {
			this.traceBuffer = traceBuffer;
			this.log = log;
			this.exceptionHandler = exceptionHandler;
			this.messageCatalog = messageCatalog;
			this.classUtils = classUtils;
		}

		public final ExceptionHandler getExceptionHandler() {
			return exceptionHandler;
		}

		public final Logger getLogger() {
			return log;
		}

		public final MessageCatalog getMessageCatalog() {
			return messageCatalog;
		}

		public ClassUtils getClassUtils() {
			return classUtils;
		}

		public TraceBuffer getTraceBuffer() {
			return traceBuffer;
		}
		
	}

}
