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
package org.simpledbm.rss.impl.platform;

import java.util.Properties;

import org.simpledbm.rss.api.exception.ExceptionHandler;
import org.simpledbm.rss.api.platform.Platform;
import org.simpledbm.rss.util.logging.Logger;
import org.simpledbm.rss.util.mcat.MessageCatalog;

public class PlatformImpl implements Platform {

	public PlatformImpl(Properties props) {
        Logger.configure(props);		
	}
	
	public ExceptionHandler getExceptionHandler(Logger log) {
		return ExceptionHandler.getExceptionHandler(log);
	}

	public Logger getLogger(String loggerName) {
		return Logger.getLogger(loggerName);
	}

	public MessageCatalog getMessageCatalog() {
		return MessageCatalog.getMessageCatalog();
	}

}
