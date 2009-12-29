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
package org.simpledbm.rss.impl.st;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import org.simpledbm.common.api.exception.ExceptionHandler;
import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.util.Dumpable;
import org.simpledbm.common.util.logging.Logger;
import org.simpledbm.common.util.mcat.MessageCatalog;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.st.StorageContainerInfo;
import org.simpledbm.rss.api.st.StorageException;
import org.simpledbm.rss.api.st.StorageManager;

/**
 * Implements the StorageManager interface.
 * 
 * @author Dibyendu Majumdar
 * @since Aug 8, 2005
 */
public final class StorageManagerImpl implements StorageManager {

    private final Logger log;
    
    @SuppressWarnings("unused")
	private final ExceptionHandler exceptionHandler;

    private final MessageCatalog mcat;
    
    private final HashMap<Integer, StorageContainerHolder> map = new HashMap<Integer, StorageContainerHolder>();

    public StorageManagerImpl(Platform platform, Properties properties) {
    	PlatformObjects po = platform.getPlatformObjects(StorageContainerFactory.LOGGER_NAME);
    	log = po.getLogger();
    	exceptionHandler = po.getExceptionHandler();
    	mcat = po.getMessageCatalog();
        mcat.addMessage("IS0022", "SIMPLEDBM-IS0022: StorageManager STOPPED");
        mcat.addMessage(
                "ES0023",
                "SIMPLEDBM-ES0023: Unexpected error occurred while closing StorageContainer {0}");
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
