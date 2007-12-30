package org.simpledbm.database;

import org.simpledbm.typesystem.api.FieldFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.impl.GenericRowFactory;

public class DatabaseRowFactory extends GenericRowFactory {
	
	Database database;

	public DatabaseRowFactory(Database database, FieldFactory fieldFactory) {
		super(fieldFactory);
		this.database = database;
	}

	@Override
	protected synchronized TypeDescriptor[] getTypeDescriptor(int keytype) {
		TypeDescriptor rowType[] = super.getTypeDescriptor(keytype);
		if (rowType == null) {
			Table table = database.getTableDefinition(keytype);
			rowType = table.getRowType();
		}
		return rowType;
	}
}
