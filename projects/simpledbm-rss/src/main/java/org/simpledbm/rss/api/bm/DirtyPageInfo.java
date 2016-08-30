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
package org.simpledbm.rss.api.bm;

import java.nio.ByteBuffer;

import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.util.Dumpable;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.wal.Lsn;

/**
 * Holds information about a dirty page.
 * 
 * @author Dibyendu Majumdar
 * @since 18-Aug-2005
 */
public final class DirtyPageInfo implements Storable, Dumpable {

    private static final int SIZE = PageId.SIZE + Lsn.SIZE;

    /**
     * PageId of the page for which information is obtained.
     */
    private final PageId pageId;

    /**
     * The recoveryLsn normally points to the oldest Lsn that may have updated
     * the page.
     */
    private Lsn recoveryLsn;

    private Lsn realRecoveryLsn;

    public DirtyPageInfo(PageId pageId, Lsn recoveryLsn, Lsn realRecoveryLsn) {
        this.pageId = pageId;
        this.recoveryLsn = recoveryLsn;
        this.realRecoveryLsn = realRecoveryLsn;
    }

    public DirtyPageInfo(ByteBuffer bb) {
        pageId = new PageId(bb);
        recoveryLsn = new Lsn(bb);
        realRecoveryLsn = recoveryLsn;
    }

    /**
     * Returns the pageId of the dirty page.
     */
    public PageId getPageId() {
        return pageId;
    }

    /**
     * Returns the recoveryLsn assigned to the page. RecoveryLsn is the LSN of
     * the oldest log record that could contain changes made to the page since
     * it was last written out.
     * 
     * @return Recovery LSN
     */
    public Lsn getRecoveryLsn() {
        return recoveryLsn;
    }

    public void setRecoveryLsn(Lsn lsn) {
        recoveryLsn = lsn;
    }

    public void setRealRecoveryLsn(Lsn lsn) {
        realRecoveryLsn = lsn;
    }

    public Lsn getRealRecoveryLsn() {
        return realRecoveryLsn;
    }

    public StringBuilder appendTo(StringBuilder sb) {
        sb.append("DirtyPageInfo(pageId=");
        pageId.appendTo(sb).append(", recoveryLsn=");
        recoveryLsn.appendTo(sb).append(", realRecoveryLsn=");
        realRecoveryLsn.appendTo(sb).append(")");
        return sb;
    }

    @Override
    public String toString() {
        return appendTo(new StringBuilder()).toString();
    }

    public void store(ByteBuffer bb) {
        pageId.store(bb);
        recoveryLsn.store(bb);
    }

    public int getStoredLength() {
        return SIZE;
    }
}
