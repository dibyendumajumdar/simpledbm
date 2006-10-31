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

package org.simpledbm.rss.api.locking;

/**
 * LockMode defines the different types of Locks available in the system. It
 * also defines the compatibility between the various locks, and the lock
 * upgrade path.
 * 
 * @author Dibyendu Majumdar
 * @since 22 July 2005
 */
public enum LockMode {
	/**
	 * Definition of various LockModes. 
	 * <ul>
	 * <li>NONE - Represents a Null lock.</li>
	 * <li>INTENTION_SHARED - Indicates the intention to read data at a lower
	 * level of granularity.</li>
	 * <li>INTENTION_EXCLUSIVE - Indicates the intention to update data at a
	 * lower level of granularity.</li>
	 * <li>SHARED - Permits readers.</li>
	 * <li>SHARED_INTENTION_EXCLUSIVE - Indicates SHARED lock at current level
	 * and intention to update data at a lower level of granularity.</li>
	 * <li>UPDATE - Indicates intention to update, Permits readers.</li>
	 * <li>EXCLUSIVE - Prevents access by other users.</li>
	 * </ul>
	 */
	NONE(0), INTENTION_SHARED(1), INTENTION_EXCLUSIVE(2), SHARED(3), SHARED_INTENTION_EXCLUSIVE(
			4), UPDATE(5), EXCLUSIVE(6);

	/**
	 * The value of the lock. Note that the lock value is used an in index
	 * into {@link #compatibilityMatrix} and {@link #conversionMatrix}. Therefore,
	 * any change in the value will require changes in the two arrays.
	 */
	private final int value;

	private LockMode(int value) {
		this.value = value;
	}

	/**
	 * Returns the numeric value associated with the LockMode.
	 * @return The numeric value associated with the LockMode.
	 */
	final int value() {
		return value;
	}

	/**
	 * Lock Compability matrix.
	 * <p> 
	 * <table border="1">
	 * <tr>
	 * <th>Mode</th>
	 * <th>NONE</th>
	 * <th>IS</th>
	 * <th>IX</th>
	 * <th>S</th>
	 * <th>SIX</th>
	 * <th>U</th>
	 * <th>X</th>
	 * </tr>
	 * <tr>
	 * <td>NONE</td>
	 * <td>Y</td>
	 * <td>Y</td>
	 * <td>Y</td>
	 * <td>Y</td>
	 * <td>Y</td>
	 * <td>Y</td>
	 * <td>Y</td>
	 * </tr>
	 * <tr>
	 * <td>Intent Shared (IS)</td>
	 * <td>Y</td>
	 * <td>Y</td>
	 * <td>Y</td>
	 * <td>Y</td>
	 * <td>Y</td>
	 * <td>N</td>
	 * <td>N</td>
	 * </tr>
	 * <tr>
	 * <td>Intent Exclusive (IX)</td>
	 * <td>Y</td>
	 * <td>Y</td>
	 * <td>Y</td>
	 * <td>N</td>
	 * <td>N</td>
	 * <td>N</td>
	 * <td>N</td>
	 * </tr>
	 * <tr>
	 * <td>Shared (S)</td>
	 * <td>Y</td>
	 * <td>Y</td>
	 * <td>N</td>
	 * <td>Y</td>
	 * <td>N</td>
	 * <td>N</td>
	 * <td>N</td>
	 * </tr>
	 * <tr>
	 * <td>Shared Intent Exclusive (SIX)</td>
	 * <td>Y</td>
	 * <td>Y</td>
	 * <td>N</td>
	 * <td>N</td>
	 * <td>N</td>
	 * <td>N</td>
	 * <td>N</td>
	 * </tr>
	 * <tr>
	 * <td>Update (U)</td>
	 * <td>Y</td>
	 * <td>N</td>
	 * <td>N</td>
	 * <td>Y</td>
	 * <td>N</td>
	 * <td>N</td>
	 * <td>N</td>
	 * </tr>
	 * <tr>
	 * <td>Exclusive (X)</td>
	 * <td>Y</td>
	 * <td>N</td>
	 * <td>N</td>
	 * <td>N</td>
	 * <td>N</td>
	 * <td>N</td>
	 * <td>N</td>
	 * </tr>
	 * </table>
	 */
	private static final boolean[][] compatibilityMatrix = {
			{ true, true, true, true, true, true, true },
			{ true, true, true, true, true, false, false },
			{ true, true, true, false, false, false, false },
			{ true, true, false, true, false, false, false },
			{ true, true, false, false, false, false, false },
			{ true, false, false, true, false, false, false },
			{ true, false, false, false, false, false, false } };

	/**
	 * Tests whether this LockMode is compatible with another. 
	 * 
	 * @param mode The other LockMode with which compatibility test is desired.
	 */
	public final boolean isCompatible(LockMode mode) {
		return compatibilityMatrix[value][mode.value()];
	}

	/**
	 * Lock Conversion matrix.
	 * <p> 
	 * <table border="1">
	 * <tr>
	 * <th>Mode</th>
	 * <th>NONE</th>
	 * <th>IS</th>
	 * <th>IX</th>
	 * <th>S</th>
	 * <th>SIX</th>
	 * <th>U</th>
	 * <th>X</th>
	 * </tr>
	 * <tr>
	 * <td>NONE</td>
	 * <td>NONE</td>
	 * <td>IS</td>
	 * <td>IX</td>
	 * <td>S</td>
	 * <td>SIX</td>
	 * <td>U</td>
	 * <td>X</td>
	 * </tr>
	 * <tr>
	 * <td>Intent Shared (IS)</td>
	 * <td>IS</td>
	 * <td>IS</td>
	 * <td>IX</td>
	 * <td>S</td>
	 * <td>SIX</td>
	 * <td>U</td>
	 * <td>X</td>
	 * </tr>
	 * <tr>
	 * <td>Intent Exclusive (IX)</td>
	 * <td>IX</td>
	 * <td>IX</td>
	 * <td>IX</td>
	 * <td>SIX</td>
	 * <td>SIX</td>
	 * <td>X</td>
	 * <td>X</td>
	 * </tr>
	 * <tr>
	 * <td>Shared (S)</td>
	 * <td>S</td>
	 * <td>S</td>
	 * <td>SIX</td>
	 * <td>S</td>
	 * <td>SIX</td>
	 * <td>U</td>
	 * <td>X</td>
	 * </tr>
	 * <tr>
	 * <td>Shared Intent Exclusive (SIX)</td>
	 * <td>SIX</td>
	 * <td>SIX</td>
	 * <td>SIX</td>
	 * <td>SIX</td>
	 * <td>SIX</td>
	 * <td>SIX</td>
	 * <td>X</td>
	 * </tr>
	 * <tr>
	 * <td>Update (U)</td>
	 * <td>U</td>
	 * <td>U</td>
	 * <td>X</td>
	 * <td>U</td>
	 * <td>SIX</td>
	 * <td>U</td>
	 * <td>X</td>
	 * </tr>
	 * <tr>
	 * <td>Exclusive (X)</td>
	 * <td>X</td>
	 * <td>X</td>
	 * <td>X</td>
	 * <td>X</td>
	 * <td>X</td>
	 * <td>X</td>
	 * <td>X</td>
	 * </tr>
	 * </table>
	 */
	private static final LockMode[][] conversionMatrix = {
			{ NONE, INTENTION_SHARED, INTENTION_EXCLUSIVE, SHARED,
					SHARED_INTENTION_EXCLUSIVE, UPDATE, EXCLUSIVE },
			{ INTENTION_SHARED, INTENTION_SHARED, INTENTION_EXCLUSIVE, SHARED,
					SHARED_INTENTION_EXCLUSIVE, UPDATE, EXCLUSIVE },
			{ INTENTION_EXCLUSIVE, INTENTION_EXCLUSIVE, INTENTION_EXCLUSIVE,
					SHARED_INTENTION_EXCLUSIVE, SHARED_INTENTION_EXCLUSIVE,
					EXCLUSIVE, EXCLUSIVE },
			{ SHARED, SHARED, SHARED_INTENTION_EXCLUSIVE, SHARED,
					SHARED_INTENTION_EXCLUSIVE, UPDATE, EXCLUSIVE },
			{ SHARED_INTENTION_EXCLUSIVE, SHARED_INTENTION_EXCLUSIVE,
					SHARED_INTENTION_EXCLUSIVE, SHARED_INTENTION_EXCLUSIVE,
					SHARED_INTENTION_EXCLUSIVE, SHARED_INTENTION_EXCLUSIVE,
					EXCLUSIVE },
			{ UPDATE, UPDATE, EXCLUSIVE, UPDATE, SHARED_INTENTION_EXCLUSIVE,
					UPDATE, EXCLUSIVE },
			{ EXCLUSIVE, EXCLUSIVE, EXCLUSIVE, EXCLUSIVE, EXCLUSIVE, EXCLUSIVE,
					EXCLUSIVE } };

	/**
	 * Determines the maximum of two LockModes.
	 * @param mode The other LockMode to which this mod is to be compared.
	 */
	public final LockMode maximumOf(LockMode mode) {
		return conversionMatrix[value][mode.value()];
	}

}
