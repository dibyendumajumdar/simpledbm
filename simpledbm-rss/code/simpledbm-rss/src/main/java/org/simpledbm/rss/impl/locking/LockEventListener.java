package org.simpledbm.rss.impl.locking;

import org.simpledbm.rss.api.locking.LockMode;

/**
 * LockEventListener allows interested clients to be notified for certain lock events.
 * @author Dibyendu Majumdar
 * @since 05 Nov 2006
 */
public interface LockEventListener {

    /** 
     * This event is generated prior to a lock request entering the wait state.
     * @param owner Prospective owner of the lock, the requester
     * @param lockable The object that is to be locked
     * @param mode The {@link LockMode} 
     */
    void beforeLockWait(Object owner, Object lockable, LockMode mode);

    /**
     * This event occurs when a lock request is granted.
     * @param owner Prospective owner of the lock, the requester
     * @param lockable The object that is to be locked
     * @param mode The {@link LockMode} 
     */
    // void lockGranted(Object owner, Object lockable, LockMode mode);
}
