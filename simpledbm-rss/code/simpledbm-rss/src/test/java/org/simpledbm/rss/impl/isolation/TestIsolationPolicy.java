package org.simpledbm.rss.impl.isolation;

import java.util.Properties;

import junit.framework.TestCase;

import org.simpledbm.rss.api.isolation.IsolationPolicy;
import org.simpledbm.rss.api.locking.LockDuration;
import org.simpledbm.rss.api.locking.LockMode;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.tx.IsolationMode;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.rss.api.tx.TransactionManager;
import org.simpledbm.rss.impl.tuple.TupleId;
import org.simpledbm.rss.impl.tuple.TupleIdFactory;
import org.simpledbm.rss.main.Server;

public class TestIsolationPolicy extends TestCase {

	public TestIsolationPolicy(String arg0) {
		super(arg0);
	}
	
	private Properties getProperties() {
		Properties properties = new Properties();
		properties.setProperty("log.ctl.1", "log/control1/ctl.a");
		properties.setProperty("log.ctl.2", "log/control2/ctl.b");
		properties.setProperty("log.groups.1.path", "log/current");
		properties.setProperty("log.archive.path", "log/archive");
		properties.setProperty("log.group.files", "3");
		properties.setProperty("log.file.size", "65536");
		properties.setProperty("log.buffer.size", "65536");
		properties.setProperty("log.buffer.limit", "4");
		properties.setProperty("log.flush.interval", "30");
		properties.setProperty("storage.basePath", "testdata/TestIsolationPolicy");
		return properties;
	}
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		Server.create(getProperties());
	}

	public void testCursorStabilityIsolationPolicy() throws Exception {
		Server server = new Server(getProperties());
		server.start();
		try {
			TransactionManager trxmgr = server.getTransactionManager();
			IsolationPolicy isolationPolicy = new CursorStabilityIsolationPolicy(); 
			Transaction trx = trxmgr.begin(IsolationMode.CURSOR_STABILITY);
			try {
				TupleIdFactory locationFactory = new TupleIdFactory();
				TupleId tid = locationFactory.newTupleId(new PageId(1, 1), 1);
				isolationPolicy.lockLocation(trx, tid, LockMode.SHARED, LockDuration.MANUAL_DURATION);
				assertEquals(LockMode.SHARED, isolationPolicy.findLocationLock(trx, tid));
				isolationPolicy.unlockLocationAfterCursorMoved(trx, tid);
				assertEquals(LockMode.NONE, isolationPolicy.findLocationLock(trx, tid));
			}
			finally {
				trx.abort();
			}
		}
		finally {
			server.shutdown();
		}
	}

	public void testRepeatableReadIsolationPolicy() throws Exception {
		Server server = new Server(getProperties());
		server.start();
		try {
			TransactionManager trxmgr = server.getTransactionManager();
			IsolationPolicy isolationPolicy = new RepeatableReadIsolationPolicy(); 
			Transaction trx = trxmgr.begin(IsolationMode.SERIALIZABLE);
			try {
				TupleIdFactory locationFactory = new TupleIdFactory();
				TupleId tid = locationFactory.newTupleId(new PageId(1, 1), 1);
				isolationPolicy.lockLocation(trx, tid, LockMode.SHARED, LockDuration.MANUAL_DURATION);
				assertEquals(LockMode.SHARED, isolationPolicy.findLocationLock(trx, tid));
				isolationPolicy.unlockLocationAfterCursorMoved(trx, tid);
				assertEquals(LockMode.SHARED, isolationPolicy.findLocationLock(trx, tid));
			}
			finally {
				trx.abort();
			}
		}
		finally {
			server.shutdown();
		}
	}
}
