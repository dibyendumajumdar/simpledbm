package org.simpledbm.database;

import java.util.ArrayList;
import java.util.Properties;

import org.simpledbm.rss.api.tx.IsolationMode;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.rss.main.Server;
import org.simpledbm.typesystem.api.FieldFactory;
import org.simpledbm.typesystem.api.RowFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.impl.DefaultFieldFactory;
import org.simpledbm.typesystem.impl.GenericRowFactory;

public class Database {

	/** Object registry id for row factory */
	final static int ROW_FACTORY_TYPE_ID = 25000;

	Server server;

	Properties properties;

	final FieldFactory fieldFactory = new DefaultFieldFactory();

	final RowFactory rowFactory = new GenericRowFactory(fieldFactory);

	ArrayList<Table> tables = new ArrayList<Table>();

	public Table addTableDefinition(String name, int containerId,
			TypeDescriptor[] rowType) {
		return new Table(this, containerId, name, rowType);
	}

	public void create() {

	}

	public void start() {

	}

	public void shutdown() {

	}

	public Server getServer() {
		return server;
	}

	public FieldFactory getFieldFactory() {
		return fieldFactory;
	}

	public RowFactory getRowFactory() {
		return rowFactory;
	}

	public void createTable(Table tableDefinition) {
		Transaction trx = server.begin(IsolationMode.READ_COMMITTED);
		boolean success = false;
		try {
			server.createTupleContainer(trx, tableDefinition.getName(),
					tableDefinition.getContainerId(), 8);
			for (Index idx : tableDefinition.getIndexes()) {
				server.createIndex(trx, idx.getName(), idx.getContainerId(), 8,
						ROW_FACTORY_TYPE_ID, idx.isUnique());
			}
			success = true;
		} finally {
			if (success)
				trx.commit();
			else
				trx.abort();
		}
	}

}
