package org.simpledbm.samples.forum.server;

import org.simpledbm.common.api.tx.IsolationMode;
import org.simpledbm.network.client.api.Session;
import org.simpledbm.network.client.api.Table;
import org.simpledbm.network.client.api.TableScan;
import org.simpledbm.typesystem.api.Row;

public class SequenceGenerator {

    final SimpleDBMContext sdbmContext;
    final String sequenceName;

    SequenceGenerator(SimpleDBMContext sdbmContext, String sequenceName) {
        this.sdbmContext = sdbmContext;
        this.sequenceName = sequenceName;
    }

    long getNextSequence() {
        long value = 0;
        Session session = sdbmContext.getSessionManager().openSession();
        try {
            session.startTransaction(IsolationMode.READ_COMMITTED);
            Table table = session.getTable(SimpleDBMContext.TABLE_SEQUENCE);
            Row row = table.getRow();
            row.setString(0, sequenceName);
            TableScan scan = table.openScan(0, row, true);
            try {
                Row fetchRow = scan.fetchNext();
                if (fetchRow != null) {
                    System.err.println("Row = " + fetchRow.toString());
                    value = fetchRow.getLong(1);
                    fetchRow.setLong(1, value - 1);
                    System.err.println("Row = " + fetchRow.toString());
                    scan.updateCurrentRow(fetchRow);
                } else {
                    throw new RuntimeException();
                }
            }
            catch (RuntimeException e) {
                e.printStackTrace();
                throw e;
            } finally {
                scan.close();
            }
            session.commit();
        } finally {
            session.close();
        }
        return value;
    }
}
