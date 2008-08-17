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

/**
 * <p>Defines the interface for the Tuple Manager module, which is responsible for
 * handling inserts, updates and deletes of tuples. A Tuple in SimpleDBM is a low-level
 * construct, and is meant to be used to implement table rows. Unlike table rows, which are
 * made up of columns, a tuple is a blob of data that can span multiple pages. The Tuple Manager
 * does not care about the contents of a tuple.</p>
 * <p>When a tuple if first inserted, it is assigned a unique Location. The Location of a
 * tuple is similar to the ROWID concept in other databases; it can be used in BTree indexes 
 * as a pointer to the Tuple.</p>
 * <p>The advantage of maintaining the Tuple as an opaque object is that it makes the
 * Tuple Manager generic and allows flexibility in the implementation of table rows. The disadvantage
 * is that the implementation cannot be optimised using knowledge about the tuple format.
 * This is most obvious in the logging operations for tuple updates; without knowledge of the
 * tuple structure, the entire before and after image of the tuple must be logged even if one
 * column has changed. This can be avoided by using a binary diff algorithm.</p>
 */
package org.simpledbm.rss.api.tuple;

