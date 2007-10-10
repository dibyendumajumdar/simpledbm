package org.simpledbm.database;

import java.nio.ByteBuffer;

import org.simpledbm.rss.api.im.IndexContainer;
import org.simpledbm.rss.api.im.IndexScan;
import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.tuple.TupleContainer;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.typesystem.api.Row;

public class TableScan {

	final Table table;
	
	final IndexScan indexScan;
	
	final Row startRow;
	
	final TupleContainer tcont;
	
	final IndexContainer icont;
	
	Row currentRow;
	
	TableScan(Transaction trx, Table table, int indexNo, Row startRow) {
		this.table = table;
		this.startRow = startRow;
		
        tcont = table.database.server.getTupleContainer(trx, table.containerId);
        Index index = table.indexes.get(indexNo);
        icont = table.database.server.getIndex(trx, index.containerId);
        indexScan = icont.openScan(trx, startRow, null, false);
	}

	public boolean fetchNext() {
		boolean okay = indexScan.fetchNext();
		if (okay) {
            Location location = indexScan.getCurrentLocation();
            // fetch tuple data
            byte[] data = tcont.read(location);
            // parse the data
            ByteBuffer bb = ByteBuffer.wrap(data);
            currentRow = table.getRow();
            currentRow.retrieve(bb);			
		}
		return okay;
	}
	
	public Row getCurrentRow() {
		return currentRow;
	}
	
	public Row getCurrentIndexRow() {
		return (Row) indexScan.getCurrentKey();
	}
	
	public void fetchCompleted(boolean matched) {
		indexScan.fetchCompleted(matched);
	}
	
	public void close() {
		indexScan.close();
	}
	
}
