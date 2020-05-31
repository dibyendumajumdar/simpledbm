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
package org.simpledbm.network.client.impl;

import org.simpledbm.network.client.api.Session;
import org.simpledbm.network.client.api.TableScan;
import org.simpledbm.network.common.api.CloseScanMessage;
import org.simpledbm.network.common.api.DeleteRowMessage;
import org.simpledbm.network.common.api.FetchNextRowMessage;
import org.simpledbm.network.common.api.FetchNextRowReply;
import org.simpledbm.network.common.api.OpenScanMessage;
import org.simpledbm.network.common.api.RequestCode;
import org.simpledbm.network.common.api.UpdateRowMessage;
import org.simpledbm.network.nio.api.Response;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.TableDefinition;

public class TableScanImpl implements TableScan {

    private final SessionImpl session;
    final TableDefinition tableDefinition;

    /**
     * Index to use for the scan.
     */
    final int indexNo;

    /**
     * Initial search row, may be null.
     */
    final Row startRow;

    /**
     * Was the scan opened for update?
     */
    final boolean forUpdate;

    /**
     * Handle for the scan.
     */
    int scanId;

    /**
     * The current row as returned by fetchNext()
     */
    Row currentRow;

    /**
     * Have we reached eof?
     */
    boolean eof;

    public TableScanImpl(SessionImpl session, TableDefinition tableDefinition,
            int indexNo, Row startRow, boolean forUpdate) {
        super();
        this.session = session;
        this.tableDefinition = tableDefinition;
        this.indexNo = indexNo;
        this.startRow = startRow;
        this.forUpdate = forUpdate;
        eof = false;
        this.scanId = open();
    }

    /**
     * Opens the scan, preparing for data to be fetched.
     */
    int open() {
        OpenScanMessage message = new OpenScanMessage(tableDefinition
                .getContainerId(), indexNo, startRow, forUpdate);
        Response response = session.sendMessage(RequestCode.OPEN_TABLESCAN,
                message);
        int scanNo = response.getData().getInt();
        //        System.err.println("Scan id = " + scanNo);
        return scanNo;
    }

    public Row fetchNext() {
        if (eof) {
            return null;
        }
        FetchNextRowMessage message = new FetchNextRowMessage(scanId);
        Response response = session.sendMessage(RequestCode.FETCH_NEXT_ROW,
                message);
        FetchNextRowReply reply = new FetchNextRowReply(getSession()
                .getSessionManager().getRowFactory(), response.getData());
        if (reply.isEof()) {
            eof = true;
            return null;
        }
        //        System.err.println("Scan row = " + reply.getRow());
        return reply.getRow();
    }

    public void updateCurrentRow(Row tableRow) {
        if (eof) {
            throw new RuntimeException("Scan has reached EOF");
        }
        UpdateRowMessage message = new UpdateRowMessage(scanId, tableDefinition.getContainerId(), tableRow);
        session.sendMessage(RequestCode.UPDATE_CURRENT_ROW, message);
    }

    public void deleteRow() {
        if (eof) {
            throw new RuntimeException("Scan has reached EOF");
        }
        DeleteRowMessage message = new DeleteRowMessage(scanId);
        session.sendMessage(RequestCode.DELETE_CURRENT_ROW, message);
    }

    public void close() {
        CloseScanMessage message = new CloseScanMessage(scanId);
        session.sendMessage(RequestCode.CLOSE_TABLESCAN, message);
    }

    public Session getSession() {
        return session;
    }
}
