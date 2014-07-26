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
/*
 * Created on: 09-Dec-2005
 * Author: Dibyendu Majumdar
 */
package org.simpledbm.rss.api.tuple;

import org.simpledbm.rss.api.loc.Location;

/**
 * When a new tuple is inserted into a container, the
 * {@link org.simpledbm.rss.api.tuple.TupleContainer#insert(Transaction, Storable)}
 * method returns a TupleInserter object which can be used to complete the
 * Insert operation. The insert operation is split up for reasons of efficiency;
 * at the time of tuple insert, it is not known whether the tuple insert can
 * proceed, for example, whether primary key constraints are satisfied. The
 * first part of the insert allocates a Location for the tuple and exclusively
 * locks it. Control is then returned to the caller to verify that the tuple
 * insert can proceed. Once it has been determined that the tuple insert can
 * proceed, the {@link #completeInsert()} method should be invoked.
 */
public interface TupleInserter {

    /**
     * Returns the Location allocated to the current insert.
     */
    Location getLocation();

    /**
     * Completes the insert, by inserting the tuple data in one or more pages.
     * The insert process is not complete until this method is called.
     */
    void completeInsert();
}
