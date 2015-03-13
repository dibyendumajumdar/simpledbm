## General assumptions ##

All locking is on data row ids only.

When a row is inserted or deleted, it is first locked in X mode, the row is inserted or deleted from data page, and only after that, indexes are modified.

Updates to indexed columns are treated as key deletes followed by key inserts. The updated row is X locked before indexes are modified.

When fetching, the index is looked up first, which causes an S or U mode lock to be placed on the row, before the data pages are accessed.

## Cursor Stability (CS) ##

CS (Cursor Stability) - as long as a cursor is positioned on a row, the row should remain locked. As soon as the cursor is moved to a different row, the lock on previous row may be released. In the aries IM paper mohan says that in System R the lock on the previous row is released only after the cursor locks the next row, but he recommends releasing the previous key lock before locking next row in order to reduce chances of deadlock. (Should the previous key lock be released after obtaining page latch?)

**Fetch:** S lock on found key or next key for manual duration. Generally, release S lock on current key before requesting lock on next key. However, In the not found case, release lock on next key immediately after the fetch (making the S lock effectively instant duration). For update cursors, same rules apply except that the lock mode is U.

**Insert:** Instant duration X lock on next key (inserted key already locked by caller). (I think that this is only required if index is unique, because in case of a non-unique index, any uncommitted deleted key is guaranteed to be on a different row). For unique indexes only, request S lock on found key, and if lock is granted, return unique constraint violation error (and release the S lock?).

**Delete:** Commit duration X lock on next key (deleted key already locked by caller).

## Repeatable Read (RR) ##

**Fetch:** S lock on found key or next key for commit duration. In case of U mode cursors, U lock (if it has not been converted to X lock already) should be downgraded to S lock before the cursor moves to the next row. (I think in the not found case, next key lock should be for manual duration only and released after the fetch is complete, as we are not trying to avoid phantoms but only check that there is not an uncommitted delete that would have been fetched).

**Insert:** Instant duration X lock on next key (inserted key already locked by caller). (I think this is only required for unique indexes). For unique indexes only, request S lock on found key for commit duration, and if lock is granted, return unique constraint violation error.

**Delete:** Same as CS.

## Serializable ##

**Fetch:** Same as RR, except that reads should lock next key in the not found case for commit duration.

**Insert** Same as RR except that for non unique indexes too, inserts should lock next key for instant duration.

**Delete** Same as RR.

## Notes ##

In Mohan's RR mode (our serializable), delete and inserts must lock next keys in order to ensure RR and avoid phantoms. This also ensures that in case of unique index, multiple entries do not appear due to transaction rollbacks. Also, in RR mode, fetch locks the next key if requested key is not present.

Mohan says that insert should lock the insert key in S mode in case the key being inserted is found and the index is unique. In RR, lock should be for commit duration.

Mohan also says that during inserts the next key should be locked for instant duration in X mode, but says that if the conditional request fails, the lock may be requested for manual duration and released after the insert to avoid lock starvation. Question: will instant duration locking work?

Delete key must perform next key locking irrespective of RR or CS.

In CS mode, fetch needs to lock next key in instant mode only when the requested key does not exist. This is to guarantee that there is not an uncommitted delete at the time of the fetch operation.