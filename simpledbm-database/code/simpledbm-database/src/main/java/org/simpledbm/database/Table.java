package org.simpledbm.database;

import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.typesystem.api.Row;

public interface Table {

	public abstract Location addRow(Transaction trx, Row tableRow);

	public abstract void updateRow(Transaction trx, Row tableRow);

	public abstract void deleteRow(Transaction trx, Row tableRow);

	public abstract TableScan openScan(Transaction trx, int indexno,
			Row startRow, boolean forUpdate);

}