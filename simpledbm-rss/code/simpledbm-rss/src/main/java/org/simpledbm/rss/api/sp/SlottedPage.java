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
 * slots within the page, other modules are freed from this onerous task, and can
 * build higher level functionality.  
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
     * Returns the total space available for slot data, including the 
     * slot table. This is equivalent to Page Size - Overhead in SlottedPage,
     * or the space available with zero slots in the page.
     * Note that this does not tell you how much free space is available, 
     * for that see {@link #getFreeSpace()}.
     * @see #getFreeSpace()
     */
    public abstract int getSpace();

    /**
     * Get the total length or size of a slot.
     * This includes the data as well as the space taken by
     * the entry in the slot table.
     */
    public abstract int getSlotLength(int slotNo);

    /**
     * Get the length of the data contained inside a slot.
     */
    public abstract int getDataLength(int slotNo);

    /**
     * Check if a particular slot has been deleted.
     * @see #delete(int)
     */
    public abstract boolean isSlotDeleted(int slotNo);

    /**
     * Inserts a new slot at first available position. The new slot will 
     * be inserted into the first available deleted slot, or if there are not any
     * deleted slots, it will be added to the end.
     */
    public abstract boolean insert(Storable item);

    /**
     * Inserts slot at specific position. If replaceMode is false,
     * existing slots will be shifted to the right. If replaceMode is 
     * false, the new slot will replace existing slot.
     */
    public abstract boolean insertAt(int slotNumber, Storable item,
            boolean replaceMode);

    /**
     * Marks a slot as deleted and release data occupied by the slot. 
     * This frees up space but does not remove the slot entry in the slot table.
     * A deleted slot can be reused when inserting new slots.
     */
    public abstract void delete(int slotNumber);

    /**
     * Removes a slot physically from the page. Both data and the slot entry in
     * the slot table are removed. The number of slots in the page is decreased by one.
     * Note that existing slots may be shifted as a result.
     */
    public abstract void purge(int slotNumber);

    /**
     * Returns slot data. The client must supply the correct type of
     * Storable factory.
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
     * Gets the amount of free space available in the page for inserting
     * new slots.
     */
    public abstract int getFreeSpace();

    /**
     * Gets the number of slots present in the page. Note that this
     * includes deleted slots as well.
     */
    public abstract int getNumberOfSlots();

    /**
     * Gets the space map page responsible for maintaining space allocation
     * data for this page.
     */
    public abstract int getSpaceMapPageNumber();

    /**
     * Sets the space map page responsible for maintaining space allocation
     * data for this page.
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
     * Reinitialize a page. All page settings must be set to
     * default and any data should be zapped.
     */
    public abstract void init();

}
