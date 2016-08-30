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
package org.simpledbm.rss.api.tx;

import org.simpledbm.rss.api.bm.BufferAccessBlock;
import org.simpledbm.rss.api.pm.Page;

/**
 * A TransactionalModule is one that is transaction aware and can participate in
 * transactions. The TransactionManager interacts with the module using this
 * interface.
 * 
 * @author Dibyendu Majumdar
 * @since 23-Aug-2005
 */
public interface TransactionalModule {

    /**
     * Performs logical undo.
     * <p>
     * Must perform undo operation and generate appropriate Compensation logs.
     * The module is not permitted to acquire or release locks during this
     * operation. It can however update pages using the buffer manager. For each
     * page update, it must generate Compensation log records that describe the
     * page update, and log them.
     * <p>
     * In case of a single page logical undo, the code will typically look like:
     * 
     * <pre>
     * FIND AND FIX PAGE THAT REQUIRES UNDO
     * GENERATE CLR
     * LOG CLR
     * PERFORM CLR ON PAGE
     * SET PAGE LSN to CLR.LSN
     * SET PAGE = DIRTY
     * UNFIX
     * </pre>
     * <p>
     * Note, however, that if single page logical undo operations are marked
     * using {@link SinglePageLogicalUndo}, then the Transaction Manager handles
     * most of the boilerplate stuff shown above, and only asks the module to
     * generate compensation log record.
     */
    void undo(Transaction trx, Undoable undoable);

    /**
     * Locates the page where the undo must be performed; used during single
     * page logical undos.
     * 
     * @see SinglePageLogicalUndo
     */
    BufferAccessBlock findAndFixPageForUndo(Undoable undoable);

    /**
     * Generates a Compensation record to represent an undo.
     * <p>
     * Since most undos are physical, it seemed errorprone to allow every undo
     * to be managed by the module. A physical undo has a pattern that can be
     * generalized, and this way there is less likelihood of an error being made
     * by a module. To support this, we differentiate between physical and
     * logical undos. By default, an Undoable change represents a physical undo.
     * If logical undo may be necessary, then the {@link LogicalUndo} interface
     * should be implemented. In case of physical undos, the module is required
     * to generate a Compensation record, which is then handled by the
     * transaction manager in a standard way.
     * 
     * @see Undoable
     * @see Compensation
     */
    Compensation generateCompensation(Undoable undoable);

    /**
     * Performs page oriented redo. Page is already exclusively latched when
     * this is called. This operation is not permitted to acquire or release
     * locks, interact with the Buffer Manager, or generate any log records.
     * This method is invoked during restart redo, or when rolling back a
     * transaction, that is, when applying Compensation log records.
     */
    void redo(Page page, Redoable loggable);

    /**
     * Perform NonTransactionRelatedOperation that is not specific to a page.
     * This operation is not permitted to acquire or release locks, interact
     * with the Buffer Manager, or generate any log records. This method is
     * invoked during restart redo only.
     */
    void redo(Loggable loggable);
}
