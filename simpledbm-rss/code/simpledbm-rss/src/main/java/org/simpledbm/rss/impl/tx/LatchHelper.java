package org.simpledbm.rss.impl.tx;

import org.simpledbm.rss.api.latch.Latch;

public class LatchHelper {

    static final int SHARED = 0;

    static final int EXCLUSIVE = 1;

    static final int FREE = -1;

    int count = 0;

    int mode = FREE;

    final Latch latch;

    public LatchHelper(Latch latch) {
        this.latch = latch;
    }

    public void exclusiveLock() {
        if (mode == EXCLUSIVE) {
            count++;
        } else if (mode == FREE) {
            latch.exclusiveLock();
            count = 1;
            mode = EXCLUSIVE;
        } else {
            throw new IllegalStateException();
        }
    }

    public void sharedLock() {
        if (mode == SHARED) {
            count++;
        } else if (mode == FREE) {
            latch.sharedLock();
            count = 1;
            mode = SHARED;
        } else {
            throw new IllegalStateException();
        }
    }

    public void unlockExclusive() {
        if (mode == EXCLUSIVE && count > 0) {
            count--;
            if (count == 0) {
                latch.unlockExclusive();
                mode = FREE;
            }
        } else {
            throw new IllegalStateException();
        }
    }

    public void unlockShared() {
        if (mode == SHARED && count > 0) {
            count--;
            if (count == 0) {
                latch.unlockShared();
                mode = FREE;
            }
        } else {
            throw new IllegalStateException();
        }
    }

}
