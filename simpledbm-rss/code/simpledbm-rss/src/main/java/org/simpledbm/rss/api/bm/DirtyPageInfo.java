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
package org.simpledbm.rss.api.bm;

import java.nio.ByteBuffer;

import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.st.Storable;
import org.simpledbm.rss.api.wal.Lsn;
import org.simpledbm.rss.util.Dumpable;

/**
 * Holds information about a dirty page. 
 * 
 * @author Dibyendu Majumdar
 * @since 18-Aug-2005
 */
public final class DirtyPageInfo implements Storable, Dumpable {
   
	private static final int SIZE = PageId.SIZE + Lsn.SIZE;
	
	private PageId pageId;

	private Lsn recoveryLsn;

	private Lsn realRecoveryLsn;

	public DirtyPageInfo() {
	}
	
	public DirtyPageInfo(PageId pageId, Lsn recoveryLsn, Lsn realRecoveryLsn) {
		this.pageId = pageId;
		this.recoveryLsn = recoveryLsn;
		this.realRecoveryLsn = realRecoveryLsn;
	}

	/**
	 * Returns the pageId of the dirty page.
	 */
	public PageId getPageId() {
		return pageId;
	}
	
	public void setPageId(PageId pageId) {
		this.pageId = pageId;
	}

	/**
	 * Returns the recoveryLsn assigned to the page. RecoveryLsn is
	 * the LSN of the oldest log record that could contain changes made to
	 * the page since it was last written out.
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
		pageId.appendTo(sb).append(", recoLsn=");
		recoveryLsn.appendTo(sb).append(", realRecoLsn=");
		realRecoveryLsn.appendTo(sb).append(")");
		return sb;
	}
	
	@Override
	public String toString() {
		return appendTo(new StringBuilder()).toString();
	}

	public void retrieve(ByteBuffer bb) {
		pageId = new PageId();
		pageId.retrieve(bb);
		recoveryLsn = new Lsn();
		recoveryLsn.retrieve(bb);
		realRecoveryLsn = new Lsn();
		realRecoveryLsn = recoveryLsn;
	}

	public void store(ByteBuffer bb) {
		pageId.store(bb);
		recoveryLsn.store(bb);
	}

	public int getStoredLength() {
		return SIZE;
	}    
}
