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
package org.simpledbm.rss.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lock free number generator similar to AtomicInteger, except that 
 * supports wrapping around of the value within a specified range.
 * 
 * @author dibyendu majumdar
 * @since 26 July 2008
 */
public class WrappingSequencer extends AtomicInteger {

	private static final long serialVersionUID = 1L;

	private final int range;
	
	/**
	 * Constructs a new WrappingSequencer initialized to wrap values
	 * between 0 and range-1. Initial value is set to 0.
	 * 
	 * @param range The value will wrap between 0 and range-1.
	 */
	public WrappingSequencer(int range) {
		super(0);
		this.range = Math.abs(range);
	}
	
	/**
	 * Gets the next value atomically. If the value hits the
	 * upper limit, it is wrapped around to 0.
	 */
	public int getNext() {
		while (true) {
			int current = get();
			int next = (current+1) % range;
			if (compareAndSet(current, next)) {
				return current;
			}
		}
	}
	
}
