package org.simpledbm.database.api;

import org.simpledbm.typesystem.api.Row;

public interface TableScan {

	public abstract boolean fetchNext();

	public abstract Row getCurrentRow();

	public abstract Row getCurrentIndexRow();

	public abstract void fetchCompleted(boolean matched);

	public abstract void close();

	public abstract void updateCurrentRow(Row tableRow);

	public abstract void deleteRow();

}