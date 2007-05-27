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
package org.simpledbm.rss.impl.sp;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.simpledbm.rss.api.sp.SlottedPage;
import org.simpledbm.rss.api.st.Storable;
import org.simpledbm.rss.util.TypeSize;
import org.simpledbm.rss.util.logging.DiagnosticLogger;
import org.simpledbm.rss.util.logging.Logger;

/**
 * SlottedPageImpl is an implementation of SlottedPage that can support page sizes
 * upto 32k. The slot offset, length, and number of slots are stored in short integers,
 * hence the restriction on page size.
 * <p>
 * The page data is organised so that the slot table starts from the top,
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
public final class SlottedPageImpl extends SlottedPage {

	static final Logger log = Logger.getLogger(SlottedPageImpl.class.getPackage().getName());

	/**
	 * This is the length of fixed length header in each page. 
	 */
	private static final int FIXED_OVERHEAD = SlottedPage.SIZE + TypeSize.SHORT * 3 + TypeSize.INTEGER * 3;

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
	public static final class Slot implements Storable {

		/**
		 * Persistent data size.
		 */
		static public final int SIZE = TypeSize.SHORT * 3;

		/**
		 * Offset within the data array where the slot's data is stored.
		 */
		private short offset = 0;

		/**
		 * Flags for the slot. Unused by SlottedPageImpl.
		 */
		private short flags = 0;

		/**
		 * Length of the slot's data. If length == 0, it means that
		 * the slot is deleted.
		 */
		private short length = 0;

		/**
		 * Default constructor
		 */
		public Slot() {
		}

		/**
		 * Copy constructor.
		 */
		public Slot(Slot slot) {
			offset = slot.offset;
			flags = slot.flags;
			length = slot.length;
		}
		
		/* (non-Javadoc)
		 * @see org.simpledbm.rss.io.Storable#retrieve(java.nio.ByteBuffer)
		 */
		public final void retrieve(ByteBuffer bb) {
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

		final void setFlags(int flags) {
			this.flags = (short) flags;
		}

		final int getLength() {
			return length;
		}

		final void setLength(int length) {
			this.length = (short) length;
		}

		final int getOffset() {
			return offset;
		}

		final void setOffset(int offset) {
			this.offset = (short) offset;
		}
		
		@Override
		public final String toString() {
			return "slot(offset=" + offset + ", length=" + length + ", flags=" + flags + ")";
		}
	}

	@Override
	public final void retrieve(ByteBuffer bb) {
		super.retrieve(bb);
		flags = bb.getShort();
		numberOfSlots = bb.getShort();
		deletedSlots = bb.getShort();
		freeSpace = bb.getInt();
		highWaterMark = bb.getInt();
		spaceMapPageNumber = bb.getInt();
		data = new byte[getSpace()];
		bb.get(data);
		slotTable.clear();
		slotTable.ensureCapacity(numberOfSlots);
		ByteBuffer bb1 = ByteBuffer.wrap(data);
		for (int slotNumber = 0; slotNumber < numberOfSlots; slotNumber++) {
			Slot slot = new Slot();
			slot.retrieve(bb1);
			slotTable.add(slotNumber, slot);
		}
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

	/**
	 * Returns the maximum number of slots that can be accomodated in a page.
	 * The slot table is restricted (arbitrarily) to 1/4th of the total
	 * space available in the page.
	 */
	@Override
	public final int getMaximumSlots() {
		return (getSpace() / 4) / Slot.SIZE;
	}

	/**
	 * Returns the total space available for page data, including the 
	 * slot table.
	 */
	@Override
	public final int getSpace() {
		if (!TESTING) {
			return super.getStoredLength() - FIXED_OVERHEAD;
		}
		return 200;
	}

	/**
	 * Default constructor.
	 */
	public SlottedPageImpl() {
		super();
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
		return slotTable.get(slotNo).getLength() + Slot.SIZE;
	}

	/**
	 * Gets the length of the data contained inside a slot.
	 */
	@Override
	public final int getDataLength(int slotNo) {
		return slotTable.get(slotNo).getLength();
	}

	/**
	 * Checks whether specified slot is deleted.
	 * A deleted slot's length is set to zero. 
	 * @see #delete(int)
	 */
	@Override
	public final boolean isSlotDeleted(int slotNo) {
		return slotTable.get(slotNo).getLength() == 0;
	}

	/**
	 * Returns the slot number where the next insert should take place.
	 */
	private int findInsertionPoint()
	{
		int slotNumber = -1;
		if (deletedSlots > 0) {
			for (slotNumber = 0; slotNumber < numberOfSlots; slotNumber++ ) {
				// Deleted tuples are identifiable by their length which is set to 0.
				if (isSlotDeleted(slotNumber)) {
					break;
				}
			}
			assert slotNumber != numberOfSlots;
		}
		else {
			slotNumber = numberOfSlots;
		}
		return slotNumber;
	}	
	
	
	/**
	 * Find the insertion point for a slot of specified length.
	 * If we have enough space, return true, else, return false.
	 * Set slot.offset to the start of the space.
	 * Note that this method is called when we already know that there 
	 * is enough space for the slot, but we do not know whether there is
	 * enough contiguous space.
	 */
	private Slot findSpace(int slotNumber, int length) {
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
		}
		else {
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

		Slot slot = new Slot();
		slot.setOffset(start_pos);
		slot.setFlags(0);
		slot.setLength(length);
		return slot;
	}

	/**
	 * Add the slot to the specified slotnumber.
	 */
	private void addSlot(int slotNumber, Storable item, Slot slot) {
		if (slotNumber == numberOfSlots) {
			numberOfSlots++;
			slotTable.ensureCapacity(numberOfSlots);
			slotTable.add(slotNumber, slot);
			freeSpace -= calculateSlotLength(slot.getLength());
		}
		else {
			if (isSlotDeleted(slotNumber)) {
				// deleted slot being reused
				deletedSlots--;
				slotTable.set(slotNumber, slot);
				freeSpace -= slot.getLength();
			}			
			else {
				// if in insert mode, shift existing tuples to the right
				numberOfSlots++;
				slotTable.ensureCapacity(numberOfSlots);
				slotTable.add(slotNumber, slot);
				freeSpace -= calculateSlotLength(slot.getLength());
			}
		}
		highWaterMark -= slot.getLength();
		ByteBuffer bb = ByteBuffer.wrap(data, slot.getOffset(), slot.getLength());
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
			System.arraycopy(data, slot.getOffset(), newdata, startPos, slot.getLength());
			slot.setOffset(startPos);
			highWaterMark -= slot.getLength();
			freeSpace -= slot.getLength();
		}
		data = newdata;
	}

	private boolean hasSpace(int slotNumber, int len) {
		if (slotNumber == numberOfSlots) {
			return freeSpace >= calculateSlotLength(len);
		}
		else {
			return freeSpace >= len;
		}
	}
	
	/**
	 * Inserts tuple. If there are any deleted tuples, we will reuse them,
	 * otherwise we will stick this tuple at the end.
	 */
	@Override
	public final boolean insert(Storable item)
	{
		assert lock.isLatchedExclusively();
		int len = item.getStoredLength();
		int slotNumber = findInsertionPoint();
		if (!hasSpace(slotNumber, len)) {
			throw new RuntimeException("Unexpected error - unable to insert data due to lack of space");
			// return false;
		}
		/* find contiguous space */
		Slot slot = findSpace(slotNumber, len);
		if (slot == null) {
			defragment();
			slot = findSpace(slotNumber, len);
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
	public final boolean insertAt(int slotNumber, Storable item, boolean replaceMode)
	{
		assert lock.isLatchedExclusively();
		int len = item.getStoredLength();
		// Calculate required space.
		// If the tuple being inserted is beyond the last tuple, then
		// we may need to fill the gap with deleted tuples.
		int requiredSpace = 0;
		if (slotNumber == numberOfSlots) {
			requiredSpace = calculateSlotLength(len);
		}
		else {
			if (isSlotDeleted(slotNumber)) {
				requiredSpace = len;
			}
			else if (replaceMode) {
				int currentLen = getDataLength(slotNumber);
				requiredSpace = len - currentLen;
			}
			else {
				requiredSpace = calculateSlotLength(len);
			}
		}
		// Do we have enough space in the page?
		// TODO: Maybe we should also reserve some free space
		if (freeSpace < requiredSpace) {
			throw new RuntimeException("Unexpected error - unable to insert data due to lack of space");
			// return false;
		}
		
		int savedFlags = 0;
		boolean restoreFlags = false;
		if (replaceMode) {
			// If we are in replaceMode,
			// we may need to delete an existing tuple
			if (slotNumber < numberOfSlots && !isSlotDeleted(slotNumber)) {
				savedFlags = slotTable.get(slotNumber).getFlags();
				restoreFlags = true;
				delete(slotNumber);
			}
		}

		/* find contiguous space */
		Slot slot = findSpace(slotNumber, len);
		if (slot == null) {
			defragment();
			slot = findSpace(slotNumber, len);
		}

		addSlot(slotNumber, item, slot);
		if (restoreFlags) {
			slotTable.get(slotNumber).setFlags(savedFlags);
		}
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
		assert lock.isLatchedExclusively();
		if (isSlotDeleted(slotNumber)) {
			return;
		}
		freeSpace += getDataLength(slotNumber);
		deletedSlots++;
		slotTable.get(slotNumber).setLength(0);
		slotTable.get(slotNumber).setFlags(0);
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
	public final void purge(int slotNumber)
	{
		assert lock.isLatchedExclusively();
		if (isSlotDeleted(slotNumber)) {
			deletedSlots--;
		}
		freeSpace += getSlotLength(slotNumber);
		slotTable.remove(slotNumber);
		numberOfSlots -= 1;
	}	
	
//	private void validateSlotNumber(int slotNumber) {
//		
//	}
	
	/**
	 * Return a pointer to tuple's data, as well as its length.
	 */
	@Override
	public final Storable get(int slotNumber, Storable item)
	{
		Slot slot = slotTable.get(slotNumber);
		ByteBuffer bb = ByteBuffer.wrap(data, slot.getOffset(), slot.getLength());
		item.retrieve(bb);
		return item;
	}	
	
	
	/**
	 * Set flags.
	 * @param slotNumber
	 * @param flags
	 */
	@Override
	public final void setFlags(int slotNumber, short flags) {
		assert lock.isLatchedExclusively();
		Slot slot = slotTable.get(slotNumber);
		slot.setFlags(flags);
	}
	
	/**
	 * Get flags.
	 * @param slotNumber
	 */
	@Override
	public final int getFlags(int slotNumber) {
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
		assert lock.isLatchedExclusively();
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
		assert lock.isLatchedExclusively();
		this.spaceMapPageNumber = spaceMapPageNumber;
	}
	
	@Override
    public int getSlotOverhead() {
        return Slot.SIZE;
    }

    @Override
	public final void dump() {
		DiagnosticLogger.log("=========================================================================");
		DiagnosticLogger.log("PAGE DUMP : " + getPageId());
		DiagnosticLogger.log("PageSize=" + getStoredLength());
		DiagnosticLogger.log("FIXED OVERHEAD=" + SlottedPageImpl.FIXED_OVERHEAD);
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
	
}
