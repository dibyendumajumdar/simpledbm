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

