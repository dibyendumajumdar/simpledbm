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

