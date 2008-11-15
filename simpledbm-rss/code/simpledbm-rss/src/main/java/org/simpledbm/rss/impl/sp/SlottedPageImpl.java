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
 *    Email  : d dot majumdar at gmail dot com ignore
 */
package org.simpledbm.rss.impl.sp;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.simpledbm.rss.api.exception.ExceptionHandler;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageException;
import org.simpledbm.rss.api.pm.PageFactory;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.pm.PageManager;
import org.simpledbm.rss.api.registry.Storable;
import org.simpledbm.rss.api.registry.StorableFactory;
import org.simpledbm.rss.api.sp.SlottedPage;
import org.simpledbm.rss.util.Dumpable;
import org.simpledbm.rss.util.TypeSize;
import org.simpledbm.rss.util.logging.DiagnosticLogger;
import org.simpledbm.rss.util.logging.Logger;
import org.simpledbm.rss.util.mcat.MessageCatalog;

/**
 * SlottedPageImpl is an implementation of SlottedPage that can support page sizes
 * up to 32k. The slot offset, length, and number of slots are stored in short integers,
 * hence the restriction on page size.
 * <p>
 * The page data is organized so that the slot table starts from the top,
 * and extends downwards, whereas, the slot data is inserted from the bottom up.
 * <pre> 
 * slot table                          slot item 
 * v                                   v                     
 * [  ][  ][  ]........................[                    ] [                  ]
 *             ^                      ^
 *             firstAvailPos          highWaterMark   
 * </pre>
 * <dl>
 * <dt>highWaterMark</dt><dd>points to the position where the next slot will _end_.
 * Slot's start point will be highWaterMark - length(slot).</dd>
 * <dt>firstAvailPos</dt><dd>tracks the end of the slot table. Any slot data
 * must follow the slot table.</dd>
 * </dl>
 *
 * @author Dibyendu Majumdar
 * @since 9 Sep 2005
 */
public final class SlottedPageImpl extends SlottedPage implements Dumpable {

    static final Logger log = Logger.getLogger(SlottedPageImpl.class
        .getPackage()
        .getName());
    
    static final ExceptionHandler exceptionHandler = ExceptionHandler.getExceptionHandler(log);

    static final MessageCatalog mcat = new MessageCatalog();

    /**
     * This is the length of fixed length header in each page. 
     */
    private static final int SLOTTEDPAGE_OVERHEAD = TypeSize.SHORT
            * 3 + TypeSize.INTEGER * 3;    
    
    /**
     * This is the length of fixed length header in each page. 
     */
    private static final int FIXED_OVERHEAD = Page.SIZE + SLOTTEDPAGE_OVERHEAD;

    /**
     * If TESTING is set to true, page space is artificially restricted to 200 bytes.
     */
    public static boolean TESTING = false;

    /**
     * Page level flags, not used by SlottedPageImpl itself.
     */
    private short flags = 0;

    /**
     * Number of slots in the page, including deleted slots.
     */
    private short numberOfSlots = 0;

    /**
     * Number of deleted slots in the page.
     */
    private short deletedSlots = 0;

    /**
     * Amount of freespace in the page.
     */
    private int freeSpace;

    /**
     * Position where the next slot must end.
     */
    private int highWaterMark;

    /**
     * Space map page that manages the space information in this page.
     * Not used by SlottedPageImpl itself.
     */
    private int spaceMapPageNumber = -1;

    /**
     * The slots and the slot data are stored in this byte array. The size of this
     * depends on the page size.
     */
    private byte[] data;

    /**
     * The slot table is maintained in memory for efficiency.
     */
    private transient ArrayList<Slot> slotTable = new ArrayList<Slot>();

    /**
     * A Slot entry in the slot table.  
     */
    public static final class Slot implements Storable, Dumpable {

        /**
         * Persistent data size.
         */
        static public final int SIZE = TypeSize.SHORT * 3;

        /**
         * Offset within the data array where the slot's data is stored.
         */
        private final short offset;

        /**
         * Flags for the slot. Unused by SlottedPageImpl.
         */
        private final short flags;

        /**
         * Length of the slot's data. If length == 0, it means that
         * the slot is deleted.
         */
        private final short length;

        
        static Slot NULL_SLOT = new Slot();
        
        /**
         * Default constructor
         */
        public Slot() {
        	offset = 0;
        	flags = 0;
        	length = 0;
        }
        
        public Slot(int flags, int offset, int length) {
        	this.offset = (short) offset;
        	this.flags = (short) flags;
        	this.length = (short) length;
        }

        /**
         * Copy constructor.
         */
        public Slot(Slot slot) {
            offset = slot.offset;
            flags = slot.flags;
            length = slot.length;
        }

        public Slot(ByteBuffer bb) {
            offset = bb.getShort();
            flags = bb.getShort();
            length = bb.getShort();
        }        
        
        /* (non-Javadoc)
         * @see org.simpledbm.rss.io.Storable#store(java.nio.ByteBuffer)
         */
        public final void store(ByteBuffer bb) {
            bb.putShort(offset);
            bb.putShort(flags);
            bb.putShort(length);
        }

        /* (non-Javadoc)
         * @see org.simpledbm.rss.io.Storable#getStoredLength()
         */
        public final int getStoredLength() {
            return SIZE;
        }

        final short getFlags() {
            return flags;
        }

        final int getLength() {
            return length;
        }

        final int getOffset() {
            return offset;
        }

        public final StringBuilder appendTo(StringBuilder sb) {
            sb
                .append("slot(offset=")
                .append(offset)
                .append(", length=")
                .append(length)
                .append(", flags=")
                .append(flags)
                .append(")");
            return sb;
        }

        @Override
        public final String toString() {
            return appendTo(new StringBuilder()).toString();
        }
    }


    SlottedPageImpl(PageManager pageFactory, int type, PageId pageId) {
		super(pageFactory, type, pageId);
		init();
	}

	SlottedPageImpl(PageManager pageFactory, PageId pageId, ByteBuffer bb) {
		super(pageFactory, pageId, bb);
        flags = bb.getShort();
        numberOfSlots = bb.getShort();
        deletedSlots = bb.getShort();
        freeSpace = bb.getInt();
        highWaterMark = bb.getInt();
        spaceMapPageNumber = bb.getInt();
        validatePageHeader();
        data = new byte[getSpace()];
        bb.get(data);
        slotTable.clear();
        slotTable.ensureCapacity(numberOfSlots);
        ByteBuffer bb1 = ByteBuffer.wrap(data);
        for (int slotNumber = 0; slotNumber < numberOfSlots; slotNumber++) {
        	Slot slot = new Slot(bb1);
            slotTable.add(slotNumber, slot);
        }
        validatePageSize();
	}

	@Override
    public final void store(ByteBuffer bb) {
        super.store(bb);
        bb.putShort(flags);
        bb.putShort(numberOfSlots);
        bb.putShort(deletedSlots);
        bb.putInt(freeSpace);
        bb.putInt(highWaterMark);
        bb.putInt(spaceMapPageNumber);
        ByteBuffer bb1 = ByteBuffer.wrap(data);
        for (int slotNumber = 0; slotNumber < numberOfSlots; slotNumber++) {
            Slot slot = slotTable.get(slotNumber);
            slot.store(bb1);
        }
        bb.put(data);
    }

    private void validatePageHeader() {
        if (numberOfSlots < 0 || numberOfSlots > getMaximumSlots()
                || freeSpace < 0 || freeSpace > getSpace() || highWaterMark < 0
                || highWaterMark > getSpace()) {
            exceptionHandler.errorThrow(this.getClass().getName(), "validate", 
            		new PageException(mcat.getMessage("EO0005") + this));
        }
    }

    private void validatePageSize() {
        int length = 0;
        for (int i = 0; i < getNumberOfSlots(); i++) {
            length += getSlotLength(i);
        }
        length += freeSpace;
        if (length != getSpace()) {
            exceptionHandler.errorThrow(this.getClass().getName(), "validatePageSize", 
            		new PageException(mcat.getMessage("EO0005") + this));
        }
    }

    /**
     * Returns the theoretical maximum number of slots that can be accomodated in a page.
     */
    private final int getMaximumSlots() {
        return getSpace() / Slot.SIZE;
    }

    /**
     * Returns the total space available for page data, including the 
     * slot table.
     */
    @Override
    public final int getSpace() {
        if (!TESTING) {
//            return super.getStoredLength() - FIXED_OVERHEAD;
            return getAvailableLength() - SLOTTEDPAGE_OVERHEAD;
        }
        // During testing it is useful to artificially restrict the usable space
        return 200;
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.pm.Page#init()
     */
    @Override
    public final void init() {
        flags = 0;
        numberOfSlots = 0;
        deletedSlots = 0;
        freeSpace = getSpace();
        highWaterMark = freeSpace;
        spaceMapPageNumber = -1;
        data = new byte[getSpace()];
        slotTable = new ArrayList<Slot>();
    }

    /**
     * Calculates the total size of a slot with a given
     * data length.
     */
    private int calculateSlotLength(int dataLen) {
        return dataLen + Slot.SIZE;
    }

    /**
     * Gets the total length or size of a slot.
     * This includes the data as well as the space taken by
     * the entry in the slot table.
     */
    @Override
    public final int getSlotLength(int slotNo) {
        validateSlotNumber(slotNo, false);
        return slotTable.get(slotNo).getLength() + Slot.SIZE;
    }

    /**
     * Gets the length of the data contained inside a slot.
     */
    @Override
    public final int getDataLength(int slotNo) {
        validateSlotNumber(slotNo, false);
        return slotTable.get(slotNo).getLength();
    }

    /**
     * Checks whether specified slot is deleted.
     * A deleted slot's length is set to zero. 
     * @see #delete(int)
     */
    @Override
    public final boolean isSlotDeleted(int slotNo) {
        validateSlotNumber(slotNo, false);
        return slotTable.get(slotNo).getLength() == 0;
    }

    /**
     * Returns the slot number where the next insert should take place.
     */
    private int findInsertionPoint() {
        int slotNumber = -1;
        if (deletedSlots > 0) {
            for (slotNumber = 0; slotNumber < numberOfSlots; slotNumber++) {
                // Deleted tuples are identifiable by their length which is set to 0.
                if (isSlotDeleted(slotNumber)) {
                    break;
                }
            }
            if (slotNumber != numberOfSlots) {
                exceptionHandler.errorThrow(this.getClass().getName(), "findInsertionPoint", 
                		new PageException(mcat.getMessage("EO0006")));
            }
        } else {
            slotNumber = numberOfSlots;
        }
        return slotNumber;
    }

    /**
     * Find the insertion point for a slot of specified length.
     * If we have enough space, return a slot object, else, return null.
     * Set slot.offset to the start of the space.
     * Note that this method is called when we already know that there 
     * is enough space for the slot, but we do not know whether there is
     * enough contiguous space.
     * @return Null if there is not enough contiguous space, a Slot object otherwise.
     */
    private Slot findSpace(int slotNumber, int length, int flags) {
        /* To find space between slots we would have to carry out some
         * expensive computation. First, we would have to sort the slots in
         * the order of slot.offset, and then scan for the largest chunk.
         * To avoid doing this, we can take the space unused after the last
         * slot, but this means that we will not use any "holes" between slots
         * until the entire page is reorganised.
         */

        int start_pos = 0;
        int first_avail_pos = 0;

        /* start_pos denotes where the tuple will start. */
        start_pos = highWaterMark - length;

        if (slotNumber == numberOfSlots || !isSlotDeleted(slotNumber)) {
            /* We have to allow for an extra offset */
            first_avail_pos = Slot.SIZE * (numberOfSlots + 1);
        } else {
            first_avail_pos = Slot.SIZE * numberOfSlots;
        }

        //		if (debug > 2) {
        //			debugout << Thread::getCurrentThread()->getName() <<
        //				": TupleMgr.findContiguousSpace: First available pos = " << first_avail_pos <<
        //				", tuple start pos = " << start_pos << "\n";
        //		}

        if (start_pos < first_avail_pos) {
            /* Not enough contiguous space */
            return null;
        }

        Slot slot = new Slot(flags, start_pos, length);
        
        return slot;
    }

    /**
     * Add the slot to the specified slotnumber.
     * Handles following cases:
     * <ol>
     * <li>A new slot to be added at the end.</li>
     * <li>A deleted slot being reused.</li>
     * <li>A new slot being inserted into the middle of the slot table. This causes existing slots to move right.</li>
     * </ol>
     */
    private void addSlot(int slotNumber, Storable item, Slot slot) {
        if (slotNumber == numberOfSlots) {
            numberOfSlots++;
            slotTable.ensureCapacity(numberOfSlots);
            slotTable.add(slotNumber, slot);
            freeSpace -= calculateSlotLength(slot.getLength());
        } else {
            if (isSlotDeleted(slotNumber)) {
                // deleted slot being reused
                deletedSlots--;
                slotTable.set(slotNumber, slot);
                freeSpace -= slot.getLength();
            } else {
                // if in insert mode, shift existing tuples to the right
                numberOfSlots++;
                slotTable.ensureCapacity(numberOfSlots);
                slotTable.add(slotNumber, slot);
                freeSpace -= calculateSlotLength(slot.getLength());
            }
        }
        highWaterMark -= slot.getLength();
        ByteBuffer bb = ByteBuffer.wrap(data, slot.getOffset(), slot
            .getLength());
        item.store(bb);
    }

    /**
     * Get rid of holes.
     */
    private void defragment() {
        byte[] newdata = new byte[getSpace()];
        highWaterMark = freeSpace = getSpace();
        freeSpace -= (Slot.SIZE * numberOfSlots);
        for (int slotNumber = 0; slotNumber < numberOfSlots; slotNumber++) {
            if (isSlotDeleted(slotNumber)) {
                continue;
            }
            Slot slot = slotTable.get(slotNumber);
            int startPos = highWaterMark - slot.getLength();
            System.arraycopy(data, slot.getOffset(), newdata, startPos, slot
                .getLength());
            slotTable.set(slotNumber, new Slot(slot.getFlags(), startPos, slot.getLength()));
            highWaterMark -= slot.getLength();
            freeSpace -= slot.getLength();
        }
        data = newdata;
    }

    private boolean hasSpace(int slotNumber, int len) {
        if (slotNumber == numberOfSlots) {
            return freeSpace >= calculateSlotLength(len);
        } else {
            return freeSpace >= len;
        }
    }

    /**
     * Inserts tuple. If there are any deleted tuples, we will reuse them,
     * otherwise we will stick this tuple at the end.
     */
    @Override
    public final boolean insert(Storable item) {
        validateLatchHeldExclusively();
        int len = item.getStoredLength();
        int slotNumber = findInsertionPoint();
        if (!hasSpace(slotNumber, len)) {
            exceptionHandler.errorThrow(this.getClass().getName(), "insert", 
            		new PageException(mcat.getMessage("EO0001", item, this)));
        }
        /* find contiguous space */
        Slot slot = findSpace(slotNumber, len, 0);
        if (slot == null) {
            defragment();
            slot = findSpace(slotNumber, len, 0);
        }

        addSlot(slotNumber, item, slot);
//		if (debug > 0) {
//			debugout << Thread::getCurrentThread()->getName() <<
//				": TupleMgr.insertTuple: Inserting tuple [" <<
//				tno << "] of length " << offset.length <<
//				" at offset " << offset.offset << "\n";
//		}
        return true;
    }

    /**
     * Insert new tuple at specific position.
     * Note that this will shift existing tuples to the right - hence
     * this function is not suitable if an allocated tupleid is immutable (such
     * as for table data). It is more for index pages where tupleid is immaterial.
     */
    @Override
    public final boolean insertAt(int slotNumber, Storable item,
            boolean replaceMode) {
        validateLatchHeldExclusively();
        validateSlotNumber(slotNumber, true);
        int len = item.getStoredLength();
        // Calculate required space.
        // If the tuple being inserted is beyond the last tuple, then
        // we may need to fill the gap with deleted tuples.
        int requiredSpace = 0;
        if (slotNumber == numberOfSlots) {
            requiredSpace = calculateSlotLength(len);
        } else {
            if (isSlotDeleted(slotNumber)) {
                requiredSpace = len;
            } else if (replaceMode) {
                int currentLen = getDataLength(slotNumber);
                requiredSpace = len - currentLen;
            } else {
                requiredSpace = calculateSlotLength(len);
            }
        }
        // Do we have enough space in the page?
        if (freeSpace < requiredSpace) {
            exceptionHandler.errorThrow(this.getClass().getName(), "insertAt", 
            	new PageException(mcat.getMessage(
                "EO0002",
                item,
                this,
                slotNumber,
                requiredSpace)));
        }

        int savedFlags = 0;
        if (replaceMode) {
            // If we are in replaceMode,
            // we may need to delete an existing tuple
            if (slotNumber < numberOfSlots && !isSlotDeleted(slotNumber)) {
                savedFlags = slotTable.get(slotNumber).getFlags();
                delete(slotNumber);
            }
        }

        /* find contiguous space */
        Slot slot = findSpace(slotNumber, len, savedFlags);
        if (slot == null) {
            defragment();
            slot = findSpace(slotNumber, len, savedFlags);
        }

        addSlot(slotNumber, item, slot);
//		if (debug > 0) {
//			debugout << Thread::getCurrentThread()->getName() <<
//				": TupleMgr.insertTupleAt: Inserting tuple [" <<
//				tno << "] of length " << p->offsets[tno].length <<
//				" at offset " << p->offsets[tno].offset << "\n";
//		}
        return true;
    }

    /**
     * Delete a tuple. This frees up space but does not meddle with tuplids.
     * Space allocated to the tuple remains unused until
     * the page is reorganized.
     */
    @Override
    public final void delete(int slotNumber) {
        validateLatchHeldExclusively();
        validateSlotNumber(slotNumber, false);
        if (isSlotDeleted(slotNumber)) {
            return;
        }
        freeSpace += getDataLength(slotNumber);
        deletedSlots++;
        slotTable.set(slotNumber, Slot.NULL_SLOT);
    }

    /**
     * Remove a tuple physically from the page.
     * Descreases tuple count.
     * Space allocated to the tuple remains unused until
     * the page is reorganized.
     * Note that this function shifts existing tuples, hence it
     * cannot be used if there is a need to make tupleids immutable (as in
     * table data). This is more for manipulating index pages where
     * tuplid is immaterial.
     */
    @Override
    public final void purge(int slotNumber) {
        validateLatchHeldExclusively();
        validateSlotNumber(slotNumber, false);
        if (isSlotDeleted(slotNumber)) {
            deletedSlots--;
        }
        freeSpace += getSlotLength(slotNumber);
        slotTable.remove(slotNumber);
        numberOfSlots -= 1;
    }

    /**
     * Validates the slot number.
     * @param slotNumber Slot number to be validated
     * @param adding Boolean flag to indcate whether to allow last+1 position
     */
    private final void validateSlotNumber(int slotNumber, boolean adding) {
        if (slotNumber < 0 || slotNumber > (numberOfSlots - (adding ? 0 : 1))) {
            exceptionHandler.errorThrow(this.getClass().getName(), "validateSlotNumber", 
            	new PageException(mcat.getMessage(
                "EO0003",
                slotNumber,
                numberOfSlots)));
        }
    }

    /**
     * Validates that the page latch has been acquired in exclusive mode.
     */
    private final void validateLatchHeldExclusively() {
        if (!lock.isLatchedExclusively()) {
            exceptionHandler.errorThrow(this.getClass().getName(), "validateLatchMode", 
            	new PageException(mcat.getMessage("EO0004")));
        }
    }

    /**
     * Return a pointer to tuple's data, as well as its length.
     */
    @Override
    public final Storable get(int slotNumber, StorableFactory storableFactory) {
        validateSlotNumber(slotNumber, false);
        Slot slot = slotTable.get(slotNumber);
        ByteBuffer bb = ByteBuffer.wrap(data, slot.getOffset(), slot
            .getLength());
        Storable item = storableFactory.getStorable(bb);
        return item;
    }

    /**
     * Set flags for a particular slot.
     * @param slotNumber
     * @param flags
     */
    @Override
    public final void setFlags(int slotNumber, short flags) {
        validateLatchHeldExclusively();
        validateSlotNumber(slotNumber, false);
        Slot slot = slotTable.get(slotNumber);
        slotTable.set(slotNumber, new Slot(flags, slot.getOffset(), slot.getLength()));
    }

    /**
     * Get flags.
     * @param slotNumber
     */
    @Override
    public final int getFlags(int slotNumber) {
        validateSlotNumber(slotNumber, false);
        Slot slot = slotTable.get(slotNumber);
        return slot.getFlags();
    }

    public final short getDeletedSlots() {
        return deletedSlots;
    }

    @Override
    public final short getFlags() {
        return flags;
    }

    @Override
    public final void setFlags(short flags) {
        validateLatchHeldExclusively();
        this.flags = flags;
    }

    @Override
    public final int getFreeSpace() {
        return freeSpace;
    }

    public final int getHighWaterMark() {
        return highWaterMark;
    }

    @Override
    public final int getNumberOfSlots() {
        return numberOfSlots;
    }

    @Override
    public final int getSpaceMapPageNumber() {
        return spaceMapPageNumber;
    }

    @Override
    public final void setSpaceMapPageNumber(int spaceMapPageNumber) {
        validateLatchHeldExclusively();
        this.spaceMapPageNumber = spaceMapPageNumber;
    }

    @Override
    public int getSlotOverhead() {
        return Slot.SIZE;
    }

    @Override
    public StringBuilder appendTo(StringBuilder sb) {

        sb.append(newline);
        sb
            .append(
                "=========================================================================")
            .append(newline);
        sb.append("PAGE DUMP : ");
        super.appendTo(sb).append(newline);
        sb.append("PageSize=").append(getStoredLength()).append(newline);
        sb
            .append("FIXED OVERHEAD=")
            .append(SlottedPageImpl.FIXED_OVERHEAD)
            .append(newline);
        sb.append("UsableSpace=").append(getSpace()).append(newline);
        // DiagnosticLogger.log("PageLsn=" + getPageLsn());
        sb.append("PageFlags=").append(getFlags()).append(newline);
        sb.append("#Slots=").append(getNumberOfSlots()).append(newline);
        sb.append("#DeletedSlots=").append(getDeletedSlots()).append(newline);
        sb.append("HighWaterMark=").append(highWaterMark).append(newline);
        sb.append("FreeSpace=").append(getFreeSpace()).append(newline);
        sb.append("SpaceMapPage=").append(getSpaceMapPageNumber()).append(newline);
        int length = 0;
        for (int i = 0; i < getNumberOfSlots(); i++) {
            sb.append("Slot#").append(i).append("=");
            slotTable.get(i).appendTo(sb).append(newline);
            length += getSlotLength(i);
        }
        length += FIXED_OVERHEAD;
        length += freeSpace;
        sb.append("Calculated PageSize=").append(length).append(newline);
        return sb;
    }

    @Override
    public String toString() {
        return appendTo(new StringBuilder()).toString();
    }

    @Override
    public final void dump() {
        DiagnosticLogger
            .log("=========================================================================");
        DiagnosticLogger.log("PAGE DUMP : " + getPageId());
        DiagnosticLogger.log("PageSize=" + getStoredLength());
        DiagnosticLogger
            .log("FIXED OVERHEAD=" + SlottedPageImpl.FIXED_OVERHEAD);
        DiagnosticLogger.log("UsableSpace=" + getSpace());
        // DiagnosticLogger.log("PageLsn=" + getPageLsn());
        DiagnosticLogger.log("PageType=" + getType());
        DiagnosticLogger.log("PageFlags=" + getFlags());
        DiagnosticLogger.log("#Slots=" + getNumberOfSlots());
        DiagnosticLogger.log("#DeletedSlots=" + getDeletedSlots());
        DiagnosticLogger.log("HighWaterMark=" + highWaterMark);
        DiagnosticLogger.log("FreeSpace=" + getFreeSpace());
        DiagnosticLogger.log("SpaceMapPage=" + getSpaceMapPageNumber());
        for (int i = 0; i < getNumberOfSlots(); i++) {
            DiagnosticLogger.log("Slot#" + i + "=" + slotTable.get(i));
        }
    }
    
    public static final class SlottedPageImplFactory implements PageFactory {
    	final PageManager pageFactory;
    	public SlottedPageImplFactory(PageManager pageFactory) {
    		this.pageFactory = pageFactory;
    	}
		public Page getInstance(int type, PageId pageId) {
			return new SlottedPageImpl(pageFactory, type, pageId);
		}
		public Page getInstance(PageId pageId, ByteBuffer bb) {
			return new SlottedPageImpl(pageFactory, pageId, bb);
		}
		public int getPageType() {
			return SlottedPageManagerImpl.TYPE_SLOTTEDPAGE;
		}
    }

}
