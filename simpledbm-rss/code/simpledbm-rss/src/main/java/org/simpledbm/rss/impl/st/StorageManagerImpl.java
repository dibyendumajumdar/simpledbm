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
package org.simpledbm.rss.impl.st;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import org.simpledbm.rss.api.exception.ExceptionHandler;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.st.StorageContainerInfo;
import org.simpledbm.rss.api.st.StorageException;
import org.simpledbm.rss.api.st.StorageManager;
import org.simpledbm.rss.util.Dumpable;
import org.simpledbm.rss.util.logging.Logger;
import org.simpledbm.rss.util.mcat.MessageCatalog;

/**
 * Implements the StorageManager interface.
 * 
 * @author Dibyendu Majumdar
 * @since Aug 8, 2005
 */
public final class StorageManagerImpl implements StorageManager {

    private static final Logger log = Logger.getLogger(StorageContainerFactory.LOGGER_NAME);
    
    private static final ExceptionHandler exceptionHandler = ExceptionHandler.getExceptionHandler(log);

    private static final MessageCatalog mcat = MessageCatalog.getMessageCatalog();
    
    private final HashMap<Integer, StorageContainerHolder> map = new HashMap<Integer, StorageContainerHolder>();

    public StorageManagerImpl(Properties properties) {
    }
    
    public final void register(int id, StorageContainer container) {
        synchronized (map) {
            map.put(id, new StorageContainerHolder(id, container));
        }
    }

    public final StorageContainer getInstance(int id) {
        StorageContainerHolder containerHolder = null;
        synchronized (map) {
            containerHolder = map.get(id);
        }
//        if (container == null) {
//            throw new StorageException(
//                    "SIMPLEDBM-ESTM-001: Unable to find an instance of StorageContainer "
//                            + id);
//        }
        if (containerHolder == null) {
            return null;
        }
        return containerHolder.getContainer();
    }

    /**
     * @throws StorageException
     * @see org.simpledbm.rss.api.st.StorageManager#remove(int)
     */
    public final void remove(int id) throws StorageException {
        StorageContainerHolder containerHolder = null;
        synchronized (map) {
            containerHolder = map.remove(id);
        }
        if (containerHolder != null) {
            containerHolder.getContainer().close();
        }
    }

    /**
     * @see org.simpledbm.rss.api.st.StorageManager#shutdown()
     */
    public final void shutdown() {
        synchronized (map) {
            StorageContainerInfo[] activeContainers = getActiveContainers();
            for (StorageContainerInfo sc : activeContainers) {
                try {
                    remove(sc.getContainerId());
                } catch (StorageException e) {
                    log.error(this.getClass().getName(), "shutdown", 
                    		mcat.getMessage("ES0023", sc), e);
                }
            }
        }
        log.info(this.getClass().getName(), "shutdown", mcat
            .getMessage("IS0022"));
    }

    public StorageContainerInfo[] getActiveContainers() {
        ArrayList<StorageContainerInfo> list = new ArrayList<StorageContainerInfo>();
        synchronized (map) {
            for (StorageContainerHolder sc : map.values()) {
                list.add(new StorageContainerInfoImpl(sc
                    .getContainer()
                    .getName(), sc.getContainerId()));
            }
        }
        return list.toArray(new StorageContainerInfo[0]);
    }

    static class StorageContainerInfoImpl implements StorageContainerInfo {

        final int containerId;

        final String name;

        public StorageContainerInfoImpl(String name, int containerId) {
            this.name = name;
            this.containerId = containerId;
        }

        public int getContainerId() {
            return containerId;
        }

        public String getName() {
            return name;
        }

        public final StringBuilder appendTo(StringBuilder sb) {
            sb.append("StorageContainerInfoImpl(name=").append(name).append(
                ", id=").append(containerId).append(")");
            return sb;
        }

        @Override
        public final String toString() {
            return appendTo(new StringBuilder()).toString();
        }
    }

    static class StorageContainerHolder implements Dumpable {

        final int containerId;

        final StorageContainer container;

        public StorageContainerHolder(int containerId,
                StorageContainer container) {
            this.containerId = containerId;
            this.container = container;
        }

        final StorageContainer getContainer() {
            return container;
        }

        final int getContainerId() {
            return containerId;
        }

        public final StringBuilder appendTo(StringBuilder sb) {
            sb
                .append("StorageContainerHolder(containerId=")
                .append(containerId)
                .append(", container=")
                .append(container)
                .append(")");
            return sb;
        }

        @Override
        public final String toString() {
            return appendTo(new StringBuilder()).toString();
        }
    }

}
