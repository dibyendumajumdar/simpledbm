/**
 * DO NOT REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Contributor(s):
 *
 * The Original Software is SimpleDBM (www.simpledbm.org).
 * The Initial Developer of the Original Software is Dibyendu Majumdar.
 *
 * Portions Copyright 2005-2014 Dibyendu Majumdar. All Rights Reserved.
 *
 * The contents of this file are subject to the terms of the
 * Apache License Version 2 (the "APL"). You may not use this
 * file except in compliance with the License. A copy of the
 * APL may be obtained from:
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the APL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the APL, the GPL or the LGPL.
 *
 * Copies of GPL and LGPL may be obtained from:
 * http://www.gnu.org/licenses/license-list.html
 */
package org.simpledbm.database.api;

import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.api.tx.IsolationMode;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.rss.main.Server;
import org.simpledbm.typesystem.api.DictionaryCache;
import org.simpledbm.typesystem.api.RowFactory;
import org.simpledbm.typesystem.api.TableDefinition;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.api.TypeFactory;
import org.simpledbm.typesystem.api.TypeSystemFactory;

/**
 * A SimpleDBM Database is a collection of Tables. The Database runs as an
 * embedded server, and provides an API for creating and maintaining tables.
 * <p>
 * A Database is created using
 * {@link DatabaseFactory#create(java.util.Properties)}. An existing Database
 * can be instantiated using
 * {@link DatabaseFactory#getDatabase(java.util.Properties)}.
 * 
 * @author dibyendu majumdar
 */
public interface Database {

    public final String LOGGER_NAME = "org.simpledbm.database";

    /**
     * Constructs a new TableDefinition object. A TableDefinition object is used
     * when creating new tables.
     * 
     * <pre>
     * Database db = ...;
     * TypeFactory ff = db.getTypeFactory();
     * TypeDescriptor employee_rowtype[] = { 
     *   ff.getIntegerType(),
     *   ff.getVarcharType(20),
     *   ff.getDateTimeType(), 
     *   ff.getNumberType(2) 
     *   };
     * TableDefinition tableDefinition = 
     *   db.newTableDefinition("employee", 1,
     *      employee_rowtype);
     * </pre>
     * 
     * @param name Name of the table
     * @param containerId ID of the container that will hold the table data
     * @param rowType A row type definition.
     * @return A TableDefinition object.
     * @see Database#getTypeFactory()
     */
    public abstract TableDefinition newTableDefinition(String name,
            int containerId, TypeDescriptor[] rowType);

    /**
     * Gets the table definition associated with the specified container ID.
     * 
     * @param containerId Id of the container
     * @return TableDefinition
     */
    public abstract TableDefinition getTableDefinition(int containerId);

    /**
     * Starts the database instance.
     */
    public abstract void start();

    /**
     * Shuts down the database instance.
     */
    public abstract void shutdown();

    /**
     * Gets the SimpleDBM RSS Server object that is managing this database.
     * 
     * @return SimpleDBM RSS Server object.
     */
    public abstract Server getServer();

    /**
     * Starts a new Transaction
     */
    public abstract Transaction startTransaction(IsolationMode isolationMode);

    public abstract TypeSystemFactory getTypeSystemFactory();

    /**
     * Returns the TypeFactory instance associated with this database. The
     * TypeFactory object can be used to create TypeDescriptors for various
     * types that can become columns in a row.
     * 
     * <pre>
     * Database db = ...;
     * TypeFactory ff = db.getTypeFactory();
     * TypeDescriptor employee_rowtype[] = { 
     *   ff.getIntegerType(),
     *   ff.getVarcharType(20),
     *   ff.getDateTimeType(), 
     *   ff.getNumberType(2) 
     *   };
     * </pre>
     */
    public abstract TypeFactory getTypeFactory();

    /**
     * Returns the RowFactory instance associated with this database. The
     * RowFactory is used to generate rows.
     * 
     * @return RowFactory instance.
     */
    public abstract RowFactory getRowFactory();

    /**
     * Returns the dictionary cache used by the database.
     */
    public abstract DictionaryCache getDictionaryCache();

    /**
     * Creates a Table and associated indexes using the information in the
     * supplied TableDefinition object. Note that the table must have a primary
     * index defined. The table creation is performed in a stand alone
     * transaction.
     * 
     * @param tableDefinition The TableDefinition object that contains
     *            information about the table to be created.
     */
    public abstract void createTable(TableDefinition tableDefinition);

    /**
     * Drops a Table and all its associated indexes.
     * 
     * @param tableDefinition The TableDefinition object that contains
     *            information about the table to be dropped.
     */
    public abstract void dropTable(TableDefinition tableDefinition);

    /**
     * Gets the table associated with the specified container ID.
     * 
     * @param trx Transaction context
     * @param containerId Id of the container
     * @return Table
     */
    public abstract Table getTable(Transaction trx, int containerId);

    public PlatformObjects getPlatformObjects();
}