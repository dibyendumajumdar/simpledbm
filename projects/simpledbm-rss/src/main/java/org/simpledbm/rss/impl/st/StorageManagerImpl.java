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
package org.simpledbm.rss.impl.st;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import org.simpledbm.common.api.exception.ExceptionHandler;
import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.util.Dumpable;
import org.simpledbm.common.util.logging.Logger;
import org.simpledbm.common.util.mcat.Message;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.common.util.mcat.MessageType;
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

    private final HashMap<Integer, StorageContainerHolder> map = new HashMap<Integer, StorageContainerHolder>();

    static Message m_IS0022 = new Message('R', 'S', MessageType.INFO, 22,
            "StorageManager STOPPED");
    static Message m_ES0023 = new Message('R', 'S', MessageType.INFO, 23,
            "Unexpected error occurred while closing StorageContainer {0}");

    public StorageManagerImpl(Platform platform, Properties properties) {
        PlatformObjects po = platform
                .getPlatformObjects(StorageContainerFactory.LOGGER_NAME);
        log = po.getLogger();
        exceptionHandler = po.getExceptionHandler();
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
                    log.error(getClass(), "shutdown",
                            new MessageInstance(m_ES0023, sc).toString(), e);
                }
            }
        }
        log.info(getClass(), "shutdown", new MessageInstance(
                m_IS0022).toString());
    }

    public StorageContainerInfo[] getActiveContainers() {
        ArrayList<StorageContainerInfo> list = new ArrayList<StorageContainerInfo>();
        synchronized (map) {
            for (StorageContainerHolder sc : map.values()) {
                list.add(new StorageContainerInfoImpl(sc.getContainer()
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
            sb.append("StorageContainerHolder(containerId=")
                    .append(containerId).append(", container=").append(
                            container).append(")");
            return sb;
        }

        @Override
        public final String toString() {
            return appendTo(new StringBuilder()).toString();
        }
    }

}
