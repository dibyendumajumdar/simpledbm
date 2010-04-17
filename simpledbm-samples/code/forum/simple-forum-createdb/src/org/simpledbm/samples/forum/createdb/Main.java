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
 *    Linking this library statically or dynamically with other modules 
 *    is making a combined work based on this library. Thus, the terms and
 *    conditions of the GNU General Public License cover the whole
 *    combination.
 *    
 *    As a special exception, the copyright holders of this library give 
 *    you permission to link this library with independent modules to 
 *    produce an executable, regardless of the license terms of these 
 *    independent modules, and to copy and distribute the resulting 
 *    executable under terms of your choice, provided that you also meet, 
 *    for each linked independent module, the terms and conditions of the 
 *    license of that module.  An independent module is a module which 
 *    is not derived from or based on this library.  If you modify this 
 *    library, you may extend this exception to your version of the 
 *    library, but you are not obligated to do so.  If you do not wish 
 *    to do so, delete this exception statement from your version.
 *
 *    Project: www.simpledbm.org
 *    Author : Dibyendu Majumdar
 *    Email  : d dot majumdar at gmail dot com ignore
 */
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
