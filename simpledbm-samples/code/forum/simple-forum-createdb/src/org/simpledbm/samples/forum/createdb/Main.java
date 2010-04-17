package org.simpledbm.samples.forum.createdb;

import java.util.Properties;

import org.simpledbm.common.api.tx.IsolationMode;
import org.simpledbm.network.client.api.Session;
import org.simpledbm.network.client.api.SessionManager;
import org.simpledbm.network.client.api.Table;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.TableDefinition;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.api.TypeFactory;

/**
 * Creates the tables used by simple forum.
 * 
 * @author dibyendumajumdar
 */
public class Main {

    public static void main(String args[]) {
        Properties properties = new Properties();
        properties
                .setProperty(
                        "logging.properties",
                        "/Users/dibyendumajumdar/simpledbm-samples-workspace/simple-forum-db/config/simpledbm.logging.properties");
        SessionManager sm = SessionManager.getSessionManager(properties,
                "localhost", 8000, 10000);
        TypeFactory ff = sm.getTypeFactory();
        Session session = sm.openSession();
        try {
            // create the forum table
            TypeDescriptor forumRowType[] = { ff.getVarcharType(30), /* name */
            ff.getVarcharType(100) /* description */
            };
            TableDefinition tableDefinition = sm.newTableDefinition("forum", 1,
                    forumRowType);
            tableDefinition.addIndex(2, "forum1.idx", new int[] { 0 }, true,
                    true);
            // create table
            session.createTable(tableDefinition);
            // create the topic table
            TypeDescriptor topicRowType[] = { ff.getVarcharType(30), /* forum name */
            ff.getLongType(), /* topic Id */
            ff.getVarcharType(100), /* title */
//            ff.getIntegerType(), /* num posts */
            ff.getVarcharType(30), /* started by */
//            ff.getVarcharType(30), /* last updated by */
//            ff.getDateTimeType() /* last updated on */
            };
            tableDefinition = sm.newTableDefinition("topic", 3, topicRowType);
            tableDefinition.addIndex(4, "topic1.idx", new int[] { 0, 1 }, true,
                    true);
            // create table
            session.createTable(tableDefinition);
            // create the post table
            TypeDescriptor postRowType[] = { ff.getVarcharType(30), /* forum name */
            ff.getLongType(), /* topic Id */
            ff.getLongType(), /* post Id */
            ff.getVarcharType(30), /* author */
            ff.getDateTimeType(), /* dateTime */
            ff.getVarcharType(4000) /* content */
            };
            tableDefinition = sm.newTableDefinition("post", 5, postRowType);
            tableDefinition.addIndex(6, "post1.idx", new int[] { 0, 1, 2 },
                    true, true);
            // create table
            session.createTable(tableDefinition);
            TypeDescriptor sequenceRowType[] = { ff.getVarcharType(30), /* sequence name */
            ff.getLongType() /* sequence */
            };
            tableDefinition = sm.newTableDefinition("sequence", 7,
                    sequenceRowType);
            tableDefinition.addIndex(8, "sequence1.idx", new int[] { 0 }, true,
                    true);
            // create table
            session.createTable(tableDefinition);

            // initialize sequences
            session.startTransaction(IsolationMode.READ_COMMITTED);

            Table table = session.getTable(7);
            Row row = table.getRow();
            row.setString(0, "topic_sequence");
            row.setLong(1, Long.MAX_VALUE);
            table.addRow(row);
            row = table.getRow();
            row.setString(0, "post_sequence");
            row.setLong(1, Long.MAX_VALUE);
            table.addRow(row);

            table = session.getTable(1);
            row = table.getRow();
            row.setString(0, "Nikon");
            row.setString(1, "Nikon Discussions");
            table.addRow(row);
            row = table.getRow();
            row.setString(0, "Canon");
            row.setString(1, "Canon Discussions");
            table.addRow(row);
            row = table.getRow();
            row.setString(0, "Leica");
            row.setString(1, "Leica Discussions");
            table.addRow(row);
            row = table.getRow();
            row.setString(0, "Ricoh");
            row.setString(1, "Ricoh Discussions");
            table.addRow(row);
            row = table.getRow();
            row.setString(0, "Zeiss");
            row.setString(1, "Zeiss Discussions");
            table.addRow(row);

            session.commit();
        } finally {
            session.close();
            sm.close();
        }
    }
}
