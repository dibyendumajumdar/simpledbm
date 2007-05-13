package org.simpledbm.typesystem.api;

import org.simpledbm.rss.api.im.IndexKeyFactory;

/**
 * A factory for generating rows.
 * 
 * @author Dibyendu Majumdar
 * @since 7 May 2007
 */
public interface RowFactory extends IndexKeyFactory {

	/**
	 * Creates a new row for a specified container ID. The container ID is
	 * used to locate the type information for the row.
	 */
	Row newRow(int containerId);
	
	/**
	 * Registers the row definition for a particular container ID.
	 * @param containerId container ID for which the row type information is being registered
	 * @param rowTypeDesc An array of types that describe the fields in the row.
	 */
	void registerRowType(int containerId, TypeDescriptor[] rowTypeDesc);

}
