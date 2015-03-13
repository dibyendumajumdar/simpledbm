### Buffer Manager ###

**lru chain performance:** LRU chain needs to be rewritten using custom linked list as Java's linked list has poor remove performance. **DONE**

**lru chain contention:** Every page fix causes LRU chain update, making the LRU chain a source of contention.

**multiple lru chains:** It would be nice to have multiple lru chains rather than the one.

### BTree Manager ###

**Unnecessary search after lock wait:** In situations where a lock wait is necessary, or during scans, the logic for checking updates to the page is simplistic. Only the LSN is checked, which means that any page modification causes a fresh search from the root.

**stores the key with each entry even for non-unique indexes:** In non-unique indexes, we should store the key once and the multiple rowids as a set. Current slotted page cannot handle this, and it would also complicate the logging and recovery.

**bulk index operation missing:** Keys can be inserted individually but this is not the most efficient way of building large indexes. Better to sort keys in some other way and then build the index bottom up.

### Lock Manager ###

**memory usage:** Minimise memory usage to improve scalability.

**reduced monitors:** Reduce number of monitors used by locking sets of buckets.

**more efficient unlocking:** Index/Tuple scans need to check the lockmode prior to releasing a lock. At present this means that there must be a find, check lockmode, and then release. It would be more efficient to combine this behaviour into the release mechanism by providing a releaseif facility. A user supplied callback would be appropriate to test whether lock should be released.

### Tuple Manager ###

**logs both before and after images of whole tuple during updates:** It would save log space if we could only store the difference. However, since Tuple Manager treats tuples as opaque objects, calculating differences is not simple.

**update selected regions of tuples:** Another way of reducing logging is to allow users to update specified regions of tuples. This would just be a manual way of specifying the changes, whereas I prefer the automatic diff mechanism.

### Write Ahead Log Manager ###

**cache log files:** Need to cache recently opened log files and avoid opening and closing them for each read request. Only affects log records no longer in the log buffer.

### Transaction Manager ###

**release read locks after prepare:** After a transaction is prepared, read locks can be released. This is particularly important if XA transaction support is to be added because in XA environments there may be a delay between preparing a transaction and committing it.

**no log records for read-only transactions:** Transactions which have not made any changes to data may be considered read only. These transactions should not require any log records. An easy way to check is to look at the transactions lastLsn - if this null, the transaction has not generated any log records.