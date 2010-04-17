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
                    value = fetchRow.getLong(1);
                    fetchRow.setLong(1, value - 1);
                    scan.updateCurrentRow(fetchRow);
                } else {
                    throw new RuntimeException();
                }
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
