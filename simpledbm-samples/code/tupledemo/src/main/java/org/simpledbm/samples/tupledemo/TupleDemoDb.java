/***
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program; if not, write to the Free Software
 *    Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *
 *    Project: www.simpledbm.org
 *    Author : Dibyendu Majumdar
 *    Email  : dibyendu@mazumdar.demon.co.uk
 */
package org.simpledbm.samples.tupledemo;

import java.nio.ByteBuffer;
import java.util.Properties;

import org.simpledbm.rss.api.im.IndexContainer;
import org.simpledbm.rss.api.im.IndexKey;
import org.simpledbm.rss.api.im.IndexKeyFactory;
import org.simpledbm.rss.api.im.IndexScan;
import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.tuple.TupleContainer;
import org.simpledbm.rss.api.tuple.TupleInserter;
import org.simpledbm.rss.api.tx.IsolationMode;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.rss.main.Server;
import org.simpledbm.typesystem.api.DataValue;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.RowFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.api.TypeFactory;
import org.simpledbm.typesystem.impl.DefaultTypeFactory;
import org.simpledbm.typesystem.impl.GenericRowFactory;
import org.simpledbm.typesystem.impl.IntegerType;
import org.simpledbm.typesystem.impl.VarcharType;

/**
 * A sample database that implements a single table, with two indexes.
 * 
 * @author Dibyendu Majumdar
 * @since 06 May 2007
 */
class TupleDemoDb {

    private Server server;

    private boolean serverStarted = false;

    /** Table container ID */
    public final static int TABLE_CONTNO = 1;
    /** Primary key index container ID */
    public final static int PKEY_CONTNO = 2;
    /** Secondary key index container ID */
    public final static int SKEY1_CONTNO = 3;

    /** Object registry id for row factory */
    final static int ROW_FACTORY_TYPE_ID = 25000;

    static Properties getServerProperties() {
        Properties properties = new Properties();
        properties.setProperty("log.ctl.1", "ctl.a");
        properties.setProperty("log.ctl.2", "ctl.b");
        properties.setProperty("log.groups.1.path", ".");
        properties.setProperty("log.archive.path", ".");
        properties.setProperty("log.group.files", "3");
        properties.setProperty("log.file.size", "16384");
        properties.setProperty("log.buffer.size", "16384");
        properties.setProperty("log.buffer.limit", "4");
        properties.setProperty("log.flush.interval", "5");
        properties.setProperty("storage.basePath", "demodata/TupleDemo1");
        return properties;
    }

    /**
     * Creates the SimpleDBM server.
     */
    public static void createServer() {
        Server.create(getServerProperties());

        TupleDemoDb server = new TupleDemoDb();
        server.startServer();
        try {
            server.createTableAndIndexes();
        } finally {
            server.shutdownServer();
        }
    }

    /**
     * Starts the SimpleDBM server instance.
     */
    public synchronized void startServer() {

        /*
         * We cannot start the server more than once
         */
        if (serverStarted) {
            throw new RuntimeException("Server is already started");
        }

        /*
         * We must always create a new server object.
         */
        server = new Server(getServerProperties());
        registerTableRowType();
        server.start();

        serverStarted = true;
    }

    /**
     * Shuts down the SimpleDBM server instance.
     */
    public synchronized void shutdownServer() {
        if (serverStarted) {
            server.shutdown();
            serverStarted = false;
            server = null;
        }
    }

    /**
     * Registers a row types for the table, primary key index, and secondary key
     * index.
     */
    void registerTableRowType() {

        final TypeFactory fieldFactory = new DefaultTypeFactory();

        final RowFactory rowFactory = new GenericRowFactory(fieldFactory);

        /**
         * Table row (id, name, surname, city)
         */
        final TypeDescriptor[] rowtype_for_mytable = new TypeDescriptor[] {
                new IntegerType(), /* primary key */
                new VarcharType(30), /* name */
                new VarcharType(30), /* surname */
                new VarcharType(20) /* city */
        };

        /**
         * Primary key (id)
         */
        final TypeDescriptor[] rowtype_for_pk = new TypeDescriptor[] { rowtype_for_mytable[0] /*
                                                                                                 * primary
                                                                                                 * key
                                                                                                 */
        };

        /**
         * Secondary key (name, surname)
         */
        final TypeDescriptor[] rowtype_for_sk1 = new TypeDescriptor[] {
                rowtype_for_mytable[2], /* surname */
                rowtype_for_mytable[1], /* name */
        };

        rowFactory.registerRowType(TABLE_CONTNO, rowtype_for_mytable);
        rowFactory.registerRowType(PKEY_CONTNO, rowtype_for_pk);
        rowFactory.registerRowType(SKEY1_CONTNO, rowtype_for_sk1);

        server.registerSingleton(ROW_FACTORY_TYPE_ID, rowFactory);
    }

    /**
     * Creates a new row object for the specified container.
     * 
     * @param containerId
     *            ID of the container
     * @return Appropriate row type
     */
    Row makeRow(int containerId) {
        RowFactory rowFactory = (RowFactory) server.getObjectRegistry()
                .getInstance(ROW_FACTORY_TYPE_ID);
        return rowFactory.newRow(containerId);
    }

    /**
     * Create a row with values that are less than any other row in the index.
     * 
     * @param containerId
     *            ID of the container
     * @return Appropriate row type
     */
    IndexKey makeMinRow(int containerId) {
        IndexKeyFactory rowFactory = (RowFactory) server.getObjectRegistry()
                .getInstance(ROW_FACTORY_TYPE_ID);
        return rowFactory.minIndexKey(containerId);
    }

    /**
     * Creates the table and associated indexes
     */
    void createTableAndIndexes() {

        Transaction trx = server.begin(IsolationMode.READ_COMMITTED);
        boolean success = false;
        try {
            server.createTupleContainer(trx, "MYTABLE.DAT", TABLE_CONTNO, 8);
            success = true;
        } finally {
            if (success)
                trx.commit();
            else
                trx.abort();
        }

        trx = server.begin(IsolationMode.READ_COMMITTED);
        success = false;
        try {
            server.createIndex(trx, "MYTABLE_PK.IDX", PKEY_CONTNO, 8,
                    ROW_FACTORY_TYPE_ID, true);
            success = true;
        } finally {
            if (success)
                trx.commit();
            else
                trx.abort();
        }

        trx = server.begin(IsolationMode.CURSOR_STABILITY);
        success = false;
        try {
            server.createIndex(trx, "MYTABLE_SKEY1.IDX", SKEY1_CONTNO, 8,
                    ROW_FACTORY_TYPE_ID, false);
            success = true;
        } finally {
            if (success)
                trx.commit();
            else
                trx.abort();
        }
    }

    /**
     * Adds a new row to the table, and updates associated indexes.
     * 
     * @param tableRow
     *            Row to be added to the table
     * @throws CloneNotSupportedException
     */
    public void addRow(int id, String name, String surname, String city) {

        Row tableRow = makeRow(TABLE_CONTNO);
        tableRow.getColumnValue(0).setInt(id);
        tableRow.getColumnValue(1).setString(name);
        tableRow.getColumnValue(2).setString(surname);
        tableRow.getColumnValue(3).setString(city);

        Row primaryKeyRow = makeRow(PKEY_CONTNO);
        // Set id
        primaryKeyRow.setColumnValue(0, (DataValue) tableRow.getColumnValue(0).cloneMe());

        Row secondaryKeyRow = makeRow(SKEY1_CONTNO);
        // Set surname as the first field
        secondaryKeyRow.setColumnValue(0, (DataValue) tableRow.getColumnValue(2).cloneMe());
        // Set name
        secondaryKeyRow.setColumnValue(1, (DataValue) tableRow.getColumnValue(1).cloneMe());

        // Start a new transaction
        Transaction trx = server.begin(IsolationMode.READ_COMMITTED);
        boolean success = false;
        try {
            TupleContainer table = server.getTupleContainer(trx, TABLE_CONTNO);
            IndexContainer primaryIndex = server.getIndex(trx, PKEY_CONTNO);
            IndexContainer secondaryIndex = server.getIndex(trx, SKEY1_CONTNO);

            // First lets create a new row and lock the location
            TupleInserter inserter = table.insert(trx, tableRow);
            // Insert the primary key - may fail with unique constraint
            // violation
            primaryIndex.insert(trx, primaryKeyRow, inserter.getLocation());
            // Insert secondary key
            secondaryIndex.insert(trx, secondaryKeyRow, inserter.getLocation());
            // Complete the insert - may be a no-op.
            inserter.completeInsert();
            success = true;
        } finally {
            if (success) {
                trx.commit();
            } else {
                trx.abort();
            }
        }
    }

    /**
     * Updates an existing row in the table, and its associated indexes.
     */
    public void updateRow(int id, String name, String surname, String city) {

        // Make new row
        Row tableRow = makeRow(TABLE_CONTNO);
        tableRow.getColumnValue(0).setInt(id);
        tableRow.getColumnValue(1).setString(name);
        tableRow.getColumnValue(2).setString(surname);
        tableRow.getColumnValue(3).setString(city);

        // New primary key
        Row primaryKeyRow = makeRow(PKEY_CONTNO);
        // Set id
        primaryKeyRow.setColumnValue(0, (DataValue) tableRow.getColumnValue(0).cloneMe());

        // New secondary key
        Row secondaryKeyRow = makeRow(SKEY1_CONTNO);
        // Set surname as the first field
        secondaryKeyRow.setColumnValue(0, (DataValue) tableRow.getColumnValue(2).cloneMe());
        // Set name
        secondaryKeyRow.setColumnValue(1, (DataValue) tableRow.getColumnValue(1).cloneMe());

        // Start a new transaction
        Transaction trx = server.begin(IsolationMode.READ_COMMITTED);
        boolean success = false;
        try {
            TupleContainer table = server.getTupleContainer(trx, TABLE_CONTNO);
            IndexContainer primaryIndex = server.getIndex(trx, PKEY_CONTNO);
            IndexContainer secondaryIndex = server.getIndex(trx, SKEY1_CONTNO);

            // Start a scan, with the primary key as argument
            IndexScan indexScan = primaryIndex.openScan(trx, primaryKeyRow,
                    null, true);
            if (indexScan.fetchNext()) {
                // Scan always return item >= search key, so let's
                // check if we had an exact match
                boolean matched = indexScan.getCurrentKey().equals(
                        primaryKeyRow);
                try {
                    if (matched) {
                        // Get location of the tuple
                        Location location = indexScan.getCurrentLocation();
                        // We need the old row data to be able to delete indexes
                        // fetch tuple data
                        byte[] data = table.read(location);
                        // parse the data
                        ByteBuffer bb = ByteBuffer.wrap(data);
                        Row oldTableRow = makeRow(TABLE_CONTNO);
                        oldTableRow.retrieve(bb);
                        // Okay, now update the table row
                        table.update(trx, location, tableRow);
                        // Update secondary indexes
                        // Old secondary key
                        Row oldSecondaryKeyRow = makeRow(SKEY1_CONTNO);
                        // Set surname as the first field
                        oldSecondaryKeyRow.setColumnValue(0, (DataValue) oldTableRow.getColumnValue(2)
                                .cloneMe());
                        // Set name
                        oldSecondaryKeyRow.setColumnValue(1, (DataValue) oldTableRow.getColumnValue(1)
                                .cloneMe());
                        // Delete old key
                        secondaryIndex
                                .delete(trx, oldSecondaryKeyRow, location);
                        // Insert new key
                        secondaryIndex.insert(trx, secondaryKeyRow, location);
                    }
                } finally {
                    indexScan.fetchCompleted(matched);
                }
            }
            success = true;
        } finally {
            if (success) {
                trx.commit();
            } else {
                trx.abort();
            }
        }
    }

    /**
     * Updates an existing row in the table, and its associated indexes.
     */
    public void deleteRow(int id) {

        // primary key
        Row primaryKeyRow = makeRow(PKEY_CONTNO);
        // Set id
        primaryKeyRow.getColumnValue(0).setInt(id);

        // Start a new transaction
        Transaction trx = server.begin(IsolationMode.READ_COMMITTED);
        boolean success = false;
        try {
            TupleContainer table = server.getTupleContainer(trx, TABLE_CONTNO);
            IndexContainer primaryIndex = server.getIndex(trx, PKEY_CONTNO);
            IndexContainer secondaryIndex = server.getIndex(trx, SKEY1_CONTNO);

            // Start a scan, with the primary key as argument
            IndexScan indexScan = primaryIndex.openScan(trx, primaryKeyRow,
                    null, true);
            if (indexScan.fetchNext()) {
                // Scan always return item >= search key, so let's
                // check if we had an exact match
                boolean matched = indexScan.getCurrentKey().equals(
                        primaryKeyRow);
                try {
                    if (matched) {
                        Location location = indexScan.getCurrentLocation();
                        // We need the old row data to be able to delete indexes
                        // fetch tuple data
                        byte[] data = table.read(location);
                        // parse the data
                        ByteBuffer bb = ByteBuffer.wrap(data);
                        Row oldTableRow = makeRow(TABLE_CONTNO);
                        oldTableRow.retrieve(bb);
                        // Delete tuple data
                        table.delete(trx, location);
                        // Delete secondary index
                        // Make secondary key
                        Row oldSecondaryKeyRow = makeRow(SKEY1_CONTNO);
                        // Set surname as the first field
                        oldSecondaryKeyRow.setColumnValue(0, (DataValue) oldTableRow.getColumnValue(2)
                                .cloneMe());
                        // Set name
                        oldSecondaryKeyRow.setColumnValue(1, (DataValue) oldTableRow.getColumnValue(1)
                                .cloneMe());
                        // Delete old key
                        secondaryIndex
                                .delete(trx, oldSecondaryKeyRow, location);
                        // Delete primary key
                        primaryIndex.delete(trx, primaryKeyRow, location);
                    }
                } finally {
                    indexScan.fetchCompleted(matched);
                }
            }
            success = true;
        } finally {
            if (success) {
                trx.commit();
            } else {
                trx.abort();
            }
        }
    }

    /**
     * Prints the contents of a single row.
     * 
     * @param tableRow
     *            Row to be printed
     */
    public void printTableRow(Row tableRow) {

        System.out.println("ID = " + tableRow.getColumnValue(0).getString() + ", Name = "
                + tableRow.getColumnValue(1).getString() + ", Surname = "
                + tableRow.getColumnValue(2).getString() + ", City = "
                + tableRow.getColumnValue(3).getString());

    }

    /**
     * Demonstrates how to scan a table using one of the indexes
     * 
     * @param keyContainerId
     */
    public void listRowsByKey(int keyContainerId) {

        Transaction trx = server.begin(IsolationMode.READ_COMMITTED);
        try {
            TupleContainer table = server.getTupleContainer(trx, TABLE_CONTNO);
            IndexContainer index = server.getIndex(trx, keyContainerId);
            IndexScan scan = index.openScan(trx, null, null, false);
            try {
                while (scan.fetchNext()) {
                    Location location = scan.getCurrentLocation();
                    // fetch tuple data
                    byte[] data = table.read(location);
                    // parse the data
                    ByteBuffer bb = ByteBuffer.wrap(data);
                    Row tableRow = makeRow(TABLE_CONTNO);
                    tableRow.retrieve(bb);
                    // do something with the row
                    printTableRow(tableRow);
                    // must invoke fetchCompleted
                    scan.fetchCompleted(true);
                }
            } finally {
                scan.close();
            }
        } finally {
            trx.abort();
        }
    }

}