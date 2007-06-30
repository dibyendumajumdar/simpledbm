package org.simpledbm.rss.api.locking;

import java.util.Properties;

/**
 * Defines factory interface for creating new LockMgr objects.
 * 
 * @author Dibyendu Majumdar
 * @since 06-Aug-05
 */
public interface LockMgrFactory {

    /**
     * Create a new LockMgr object using specified parameters.
     * @param props Properties that specify parameters for the Lock Manager.
     * @return A LockMgr object.
     */
    LockManager create(Properties props);

}
