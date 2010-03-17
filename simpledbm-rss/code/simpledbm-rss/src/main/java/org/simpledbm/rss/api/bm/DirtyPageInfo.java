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
