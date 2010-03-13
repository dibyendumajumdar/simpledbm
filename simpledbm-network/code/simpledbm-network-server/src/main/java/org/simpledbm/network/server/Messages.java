package org.simpledbm.network.server;

import org.simpledbm.common.util.mcat.Message;
import org.simpledbm.common.util.mcat.MessageType;

class Messages {
    static final Message timedOutMessage = new Message('N', 'S', MessageType.ERROR, 1, "Session has timed out");
    static final Message noSuchTableMessage = new Message('N', 'S', MessageType.ERROR, 2, "Table {0} does not exist");
    static final Message noSuchTableScanMessage = new Message('N', 'S', MessageType.ERROR, 3, "TableScan {0} does not exist");
	static final Message noSuchSession = new Message('N', 'S', MessageType.ERROR, 4, "Session {0} does not exist");
	static final Message transactionActive = new Message('N', 'S', MessageType.ERROR, 5, "Transaction {0} is active");
	static final Message noActiveTransaction = new Message('N', 'S', MessageType.ERROR, 6, "There is no active transaction");

	
	static final Message UnexpectedError = new Message('N', 'S', MessageType.ERROR, 99, "Unexpected error while converting message to UTF-8 format");	
}
