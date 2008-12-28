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

import java.util.HashMap;
import java.util.Properties;

import org.simpledbm.rss.api.exception.ExceptionHandler;
import org.simpledbm.rss.api.tx.TransactionException;
import org.simpledbm.rss.api.tx.TransactionManager;
import org.simpledbm.rss.api.tx.TransactionalModule;
import org.simpledbm.rss.api.tx.TransactionalModuleRegistry;
import org.simpledbm.rss.util.logging.Logger;
import org.simpledbm.rss.util.mcat.MessageCatalog;

public final class TransactionalModuleRegistryImpl implements
        TransactionalModuleRegistry {

    static final Logger log = Logger
        .getLogger(TransactionManager.LOGGER_NAME);
    static final MessageCatalog mcat = MessageCatalog.getMessageCatalog();
    static final ExceptionHandler exceptionHandler = ExceptionHandler.getExceptionHandler(log);

    private final HashMap<Short, TransactionalModule> moduleMap = new HashMap<Short, TransactionalModule>();

    public TransactionalModuleRegistryImpl(Properties p) {
    }
    
    public final synchronized void registerModule(int moduleId,
            TransactionalModule module) {
        moduleMap.put((short) moduleId, module);
    }

    public final synchronized TransactionalModule getModule(int moduleId) {
        TransactionalModule module = moduleMap.get((short) moduleId);
        if (module == null) {
            exceptionHandler.errorThrow(this.getClass().getName(), "getModule", 
            		new TransactionException(mcat.getMessage("EX0001", moduleId)));
        }
        return module;
    }

}
