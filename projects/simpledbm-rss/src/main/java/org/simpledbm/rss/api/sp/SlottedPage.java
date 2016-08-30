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
package org.simpledbm.rss.api.sp;

import java.nio.ByteBuffer;

import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.api.registry.StorableFactory;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.pm.PageManager;

/**
 * A SlottedPage is named as such because it contains a slot table, and supports
 * multiple slots of data. By abstracting out the functionality of maintaining
 * slots within the page, other modules are freed from this onerous task, and
 * can build higher level functionality.
 * 
 * @author Dibyendu Majumdar
 * @since Oct 5, 2005
 */
public abstract class SlottedPage extends Page {

    protected SlottedPage(PageManager pageFactory, int type, PageId pageId) {
        super(pageFactory, type, pageId);
    }

    protected SlottedPage(PageManager pageFactory, PageId pageId, ByteBuffer bb) {
        super(pageFactory, pageId, bb);
    }

    /**
     * Returns the total space available for slot data, including the slot
     * table. This is equivalent to Page Size - Overhead in SlottedPage, or the
     * space available with zero slots in the page. Note that this does not tell
     * you how much free space is available, for that see
     * {@link #getFreeSpace()}.
     * 
     * @see #getFreeSpace()
     */
    public abstract int getSpace();

    /**
     * Get the total length or size of a slot. This includes the data as well as
     * the space taken by the entry in the slot table.
     */
    public abstract int getSlotLength(int slotNo);

    /**
     * Get the length of the data contained inside a slot.
     */
    public abstract int getDataLength(int slotNo);

    /**
     * Check if a particular slot has been deleted.
     * 
     * @see #delete(int)
     */
    public abstract boolean isSlotDeleted(int slotNo);

    /**
     * Inserts a new slot at first available position. The new slot will be
     * inserted into the first available deleted slot, or if there are not any
     * deleted slots, it will be added to the end.
     */
    public abstract boolean insert(Storable item);

    /**
     * Inserts slot at specific position. If replaceMode is false, existing
     * slots will be shifted to the right. If replaceMode is false, the new slot
     * will replace existing slot.
     */
    public abstract boolean insertAt(int slotNumber, Storable item,
            boolean replaceMode);

    /**
     * Marks a slot as deleted and release data occupied by the slot. This frees
     * up space but does not remove the slot entry in the slot table. A deleted
     * slot can be reused when inserting new slots.
     */
    public abstract void delete(int slotNumber);

    /**
     * Removes a slot physically from the page. Both data and the slot entry in
     * the slot table are removed. The number of slots in the page is decreased
     * by one. Note that existing slots may be shifted as a result.
     */
    public abstract void purge(int slotNumber);

    /**
     * Returns slot data. The client must supply the correct type of Storable
     * factory.
     */
    public abstract Storable get(int slotNumber, StorableFactory storableFactory);

    /**
     * Sets slot specific flags.
     */
    public abstract void setFlags(int slotNumber, short flags);

    /**
     * Gets slot specific flags.
     */
    public abstract int getFlags(int slotNumber);

    /**
     * Gets page level flags.
     */
    public abstract short getFlags();

    /**
     * Sets page level flags.
     */
    public abstract void setFlags(short flags);

    /**
     * Gets the amount of free space available in the page for inserting new
     * slots.
     */
    public abstract int getFreeSpace();

    /**
     * Gets the number of slots present in the page. Note that this includes
     * deleted slots as well.
     */
    public abstract int getNumberOfSlots();

    /**
     * Gets the space map page responsible for maintaining space allocation data
     * for this page.
     */
    public abstract int getSpaceMapPageNumber();

    /**
     * Sets the space map page responsible for maintaining space allocation data
     * for this page.
     */
    public abstract void setSpaceMapPageNumber(int spaceMapPageNumber);

    /**
     * Dumps contents of the page.
     */
    public abstract void dump();

    /**
     * Returns the overhead of a single slot.
     */
    public abstract int getSlotOverhead();

    /**
     * Reinitialize a page. All page settings must be set to default and any
     * data should be zapped.
     */
    public abstract void init();

}
