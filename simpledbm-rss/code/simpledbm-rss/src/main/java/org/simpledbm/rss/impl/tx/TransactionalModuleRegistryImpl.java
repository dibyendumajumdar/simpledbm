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
package org.simpledbm.rss.impl.tx;

import java.util.HashMap;
import java.util.Properties;

import org.simpledbm.common.api.exception.ExceptionHandler;
import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.util.logging.Logger;
import org.simpledbm.common.util.mcat.Message;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.common.util.mcat.MessageType;
import org.simpledbm.rss.api.tx.TransactionException;
import org.simpledbm.rss.api.tx.TransactionManager;
import org.simpledbm.rss.api.tx.TransactionalModule;
import org.simpledbm.rss.api.tx.TransactionalModuleRegistry;

public final class TransactionalModuleRegistryImpl implements
        TransactionalModuleRegistry {

    final Logger log;
    final ExceptionHandler exceptionHandler;

    private final HashMap<Short, TransactionalModule> moduleMap = new HashMap<Short, TransactionalModule>();
    static Message m_EX0001 = new Message('R', 'X', MessageType.ERROR, 1,
            "Unknown transaction module {0}");

    public TransactionalModuleRegistryImpl(Platform platform, Properties p) {
        PlatformObjects po = platform
                .getPlatformObjects(TransactionManager.LOGGER_NAME);
        log = po.getLogger();
        exceptionHandler = po.getExceptionHandler();
    }

    public final synchronized void registerModule(int moduleId,
            TransactionalModule module) {
        moduleMap.put((short) moduleId, module);
    }

    public final synchronized TransactionalModule getModule(int moduleId) {
        TransactionalModule module = moduleMap.get((short) moduleId);
        if (module == null) {
            exceptionHandler.errorThrow(this.getClass(), "getModule",
                    new TransactionException(new MessageInstance(m_EX0001,
                            moduleId)));
        }
        return module;
    }

}
