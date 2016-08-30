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
package org.simpledbm.rss.api.pm;

import java.nio.ByteBuffer;

import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.util.Dumpable;
import org.simpledbm.common.util.TypeSize;

/**
 * Each page in the database is uniquely identified by a pageid consisting of
 * storage container id and the page number.
 * <p>
 * Immutable.
 * 
 * @author Dibyendu Majumdar
 * @since 19-Aug-2005
 */
public final class PageId implements Comparable<PageId>, Storable, Dumpable {

    /**
     * Size of PageId in bytes.
     */
    public final static int SIZE = TypeSize.INTEGER * 2;

    private final int containerId;
    private final int pageNumber;

    public PageId() {
        containerId = -1;
        pageNumber = -1;
    }

    public PageId(int containerId, int pageNumber) {
        this.containerId = containerId;
        this.pageNumber = pageNumber;
    }

    public PageId(PageId pageId) {
        this.containerId = pageId.containerId;
        this.pageNumber = pageId.pageNumber;
    }

    public PageId(ByteBuffer bb) {
        containerId = bb.getInt();
        pageNumber = bb.getInt();
    }

    public final int compareTo(PageId pageId) {
        if (containerId == pageId.containerId) {
            if (pageNumber == pageId.pageNumber)
                return 0;
            else if (pageNumber > pageId.pageNumber)
                return 1;
            else
                return -1;
        } else if (containerId > pageId.containerId)
            return 1;
        else
            return -1;
    }

    public final void store(ByteBuffer bb) {
        bb.putInt(containerId);
        bb.putInt(pageNumber);
    }

    public final int getStoredLength() {
        return SIZE;
    }

    public final int getContainerId() {
        return containerId;
    }

    public final int getPageNumber() {
        return pageNumber;
    }

    public final boolean isNull() {
        return containerId == -1 && pageNumber == -1;
    }

    public final StringBuilder appendTo(StringBuilder sb) {
        return sb.append("PageId(").append(containerId).append(",").append(
                pageNumber).append(")");
    }

    public final String toString() {
        return appendTo(new StringBuilder()).toString();
    }

    public int hashCode() {
        final int PRIME = 31;
        int result = 1;
        result = PRIME * result + containerId;
        result = PRIME * result + pageNumber;
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final PageId other = (PageId) obj;
        if (containerId != other.containerId)
            return false;
        if (pageNumber != other.pageNumber)
            return false;
        return true;
    }

}
