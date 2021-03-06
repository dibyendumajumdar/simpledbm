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
package org.simpledbm.rss.impl.im.btree;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.simpledbm.common.api.exception.ExceptionHandler;
import org.simpledbm.common.api.exception.SimpleDBMException;
import org.simpledbm.common.api.key.IndexKey;
import org.simpledbm.common.api.key.IndexKeyFactory;
import org.simpledbm.common.api.locking.LockMode;
import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.api.registry.ObjectFactory;
import org.simpledbm.common.api.registry.ObjectRegistry;
import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.api.registry.StorableFactory;
import org.simpledbm.common.api.tx.IsolationMode;
import org.simpledbm.common.tools.diagnostics.TraceBuffer;
import org.simpledbm.common.util.Dumpable;
import org.simpledbm.common.util.TypeSize;
import org.simpledbm.common.util.logging.DiagnosticLogger;
import org.simpledbm.common.util.logging.Logger;
import org.simpledbm.common.util.mcat.Message;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.common.util.mcat.MessageType;
import org.simpledbm.rss.api.bm.BufferAccessBlock;
import org.simpledbm.rss.api.bm.BufferManager;
import org.simpledbm.rss.api.fsm.FreeSpaceChecker;
import org.simpledbm.rss.api.fsm.FreeSpaceCursor;
import org.simpledbm.rss.api.fsm.FreeSpaceManager;
import org.simpledbm.rss.api.fsm.FreeSpaceMapPage;
import org.simpledbm.rss.api.im.IndexContainer;
import org.simpledbm.rss.api.im.IndexException;
import org.simpledbm.rss.api.im.IndexManager;
import org.simpledbm.rss.api.im.IndexScan;
import org.simpledbm.rss.api.im.UniqueConstraintViolationException;
import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.loc.LocationFactory;
import org.simpledbm.rss.api.locking.LockDuration;
import org.simpledbm.rss.api.locking.LockException;
import org.simpledbm.rss.api.locking.util.LockAdaptor;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.sp.SlottedPage;
import org.simpledbm.rss.api.sp.SlottedPageManager;
import org.simpledbm.rss.api.tx.BaseLoggable;
import org.simpledbm.rss.api.tx.BaseTransactionalModule;
import org.simpledbm.rss.api.tx.Compensation;
import org.simpledbm.rss.api.tx.LoggableFactory;
import org.simpledbm.rss.api.tx.LogicalUndo;
import org.simpledbm.rss.api.tx.MultiPageRedo;
import org.simpledbm.rss.api.tx.Redoable;
import org.simpledbm.rss.api.tx.Savepoint;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.rss.api.tx.TransactionalCursor;
import org.simpledbm.rss.api.tx.TransactionalModuleRegistry;
import org.simpledbm.rss.api.tx.Undoable;
import org.simpledbm.rss.api.wal.Lsn;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * B-link Tree implementation is based upon the algorithms described in <cite>
 * Ibrahim Jaluta, Seppo Sippu and Eljas Soisalon-Soininen. Concurrency control
 * and recovery for balanced B-link trees. The VLDB Journal, Volume 14, Issue 2
 * (April 2005), Pages: 257 - 277, ISSN:1066-8888.</cite> There are some
 * variations from the published algorithms - these are noted at appropriate
 * places within the code.
 * <p>
 * <h2>Structure of the B-link Tree</h2>
 * <ol>
 * <li>The tree is contained in a container of fixed size pages. The first page
 * (pagenumber = 0) of the container is a header page. The second page
 * (pagenumber = 1) is the first space map page. The third page (pagenumber = 2)
 * is always allocated as the root page of the tree. The root page never
 * changes.</li>
 * <li>Pages at all levels are linked to their right siblings.</li>
 * <li>In leaf pages, an extra item called the high key is present. In index
 * pages, the last key acts as the highkey. All keys in a page are guaranteed to
 * be &lt;= than the highkey. Note that in leaf pages the highkey may not be the
 * same as the last key in the page.</li>
 * <li>In index pages, each key contains a pointer to a child page. The child
 * page contains keys &lt;= to the key in the index page. The highkey of the
 * child page will match the index key if the child is a direct child. The
 * highkey of the child page will be &lt; than the index key if the child has a
 * sibling that is an indirect child.</li>
 * <li>All pages other than root must have at least two items (in addition to
 * highkeys in leaf pages).</li>
 * <li>The rightmost key at any level is a special key containing logical
 * INFINITY. Initially, the empty tree contains this key only. As the tree grows
 * through splitting of pages, the INFINITY key is carried forward to the
 * rightmost pages at each level of the tree. This key can never be deleted from
 * the tree.</li>
 * <li>A page is considered as safe if it is not about to underflow and is not
 * about to overflow. A page is about to underflow if it is the root node and
 * has 1 child or it is any other node and has two keys. A page is about to
 * overflow if it cannot accommodate a new key.</li>
 * </ol>
 * 
 * @author Dibyendu Majumdar
 * @since 18-Sep-2005
 */
public final class BTreeIndexManagerImpl extends BaseTransactionalModule
        implements IndexManager {

    /*
     * Notes: 
     * 1. If we want to maintain left sibling pointers in pages,
     * then during merge operations, we need to access the page further to the
     * right of right sibling, and update this page as well. 
     * 2. When copying keys from one page to another it is not necessary 
     * to instantiate keys. A more efficient way would be to copy raw data. 
     * Current method is inefficient.
     */

    private static final short MODULE_ID = 4;

    private static final short TYPE_BASE = 40;
    private static final short TYPE_SPLIT_OPERATION = TYPE_BASE + 1;
    private static final short TYPE_MERGE_OPERATION = TYPE_BASE + 3;
    private static final short TYPE_LINK_OPERATION = TYPE_BASE + 4;
    private static final short TYPE_UNLINK_OPERATION = TYPE_BASE + 5;
    private static final short TYPE_REDISTRIBUTE_OPERATION = TYPE_BASE + 6;
    private static final short TYPE_INCREASETREEHEIGHT_OPERATION = TYPE_BASE + 7;
    private static final short TYPE_DECREASETREEHEIGHT_OPERATION = TYPE_BASE + 8;
    private static final short TYPE_INSERT_OPERATION = TYPE_BASE + 9;
    private static final short TYPE_UNDOINSERT_OPERATION = TYPE_BASE + 10;
    private static final short TYPE_DELETE_OPERATION = TYPE_BASE + 11;
    private static final short TYPE_UNDODELETE_OPERATION = TYPE_BASE + 12;
    private static final short TYPE_LOADPAGE_OPERATION = TYPE_BASE + 13;
    private static final short TYPE_NEWREDISTRIBUTE_OPERATION = TYPE_BASE + 14;

    /**
     * Space map value for a used BTree page. The space map can have two
     * possible values - 1 means used, and 0 means unused.
     */
    private static final int PAGE_SPACE_UNUSED = 0;

    /**
     * Space map value for a used BTree page. The space map can have two
     * possible values - 1 means used, and 0 means unused.
     */
    private static final int PAGE_SPACE_USED = 1;

    /**
     * Root page is always the third page in a container.
     */
    private static final int ROOT_PAGE_NUMBER = 2;

    /**
     * The first slot in a btree node page is occupied by a special header
     * record.
     */
    private static final int HEADER_KEY_POS = 0;

    /**
     * The keys start at slot position 1, because the first slot is occupied by
     * a header record.
     */
    private static final int FIRST_KEY_POS = 1;

    final ObjectRegistry objectFactory;

    final LoggableFactory loggableFactory;

    final FreeSpaceManager spaceMgr;

    final BufferManager bufmgr;

    final SlottedPageManager spMgr;

    final LockAdaptor lockAdaptor;

    final PlatformObjects po;

    final Logger log;

    final ExceptionHandler exceptionHandler;

    final TraceBuffer tracer;

    /**
     * Setting this flag to 1 artificially limits the number of keys in each
     * page. This is useful for forcing page splits etc. when testing.
     * 
     * @see BTreeNode#canAccomodate(org.simpledbm.rss.impl.im.btree.BTreeIndexManagerImpl.IndexItem)
     * @see BTreeNode#canMergeWith(org.simpledbm.rss.impl.im.btree.BTreeIndexManagerImpl.BTreeNode)
     * @see BTreeNode#getSplitKey()
     */
    private static int testingFlag = 0;

    public static final int TEST_MODE_LIMIT_MAX_KEYS_PER_PAGE = 1;

    /**
     * During test mode, force a node to have a maximum of 8 keys. This causes
     * more frequent page splits and merges.
     */
    private static final int TEST_MODE_MAX_KEYS = 8;

    /**
     * During test mode, the median key is defined as the 5th key (4).
     */
    private static final int TEST_MODE_SPLIT_KEY = 4;

    private static final boolean Validating = false;
    private static final boolean QuickValidate = false;

    // BTree Index Manager messages
    static Message m_EB0001 = new Message('R', 'B', MessageType.ERROR, 1,
            "Unexpected error - missing child pointer in parent node");
    static Message m_EB0002 = new Message('R', 'B', MessageType.ERROR, 2,
            "Unable to allocate a new page in the B-Tree container");
    static Message m_WB0003 = new Message(
            'R',
            'B',
            MessageType.WARN,
            3,
            "Unique constraint would be violated by insertion of (key=[{0}], location=[{1}]): conflicts with (key=[{2}], location=[{3}])");
    static Message m_EB0004 = new Message('R', 'B', MessageType.ERROR, 4,
            "Unexpected error - key {0} not found");
    static Message m_EB0005 = new Message('R', 'B', MessageType.ERROR, 5,
            "Unexpected error - current key k1={0} does not match expected key k2={1}");
    static Message m_EB0006 = new Message(
            'R',
            'B',
            MessageType.ERROR,
            6,
            "Unexpected error - search result returned null, B-Tree may be corrupt : search key = {0}");
    static Message m_EB0007 = new Message('R', 'B', MessageType.ERROR, 7,
            "Unexpected error - while attempting to locate the split key in page {0}");
    static Message m_EB0008 = new Message('R', 'B', MessageType.ERROR, 8,
            "Unexpected error - invalid binary search result while searching for {0}");
    static Message m_EB0009 = new Message('R', 'B', MessageType.ERROR, 9,
            "Unexpected error - leaf page {0} encountered when expecting an index page");
    static Message m_EB0010 = new Message('R', 'B', MessageType.ERROR, 10,
            "Index item {0} is not setup as leaf");
    static Message m_WB0011 = new Message('R', 'B', MessageType.WARN, 11,
            "fetchCompleted() has not been called after fetchNext()");
    static Message m_EB0012 = new Message('R', 'B', MessageType.ERROR, 12,
            "Unexpected error - exception caught");
    static Message m_EB0013 = new Message('R', 'B', MessageType.ERROR, 13,
            "Cannot insert or delete logical max key");
    static Message m_EB0014 = new Message('R', 'B', MessageType.ERROR, 14,
            "The BTreeNode no longer refers to the correct page");
    static Message m_EB0015 = new Message('R', 'B', MessageType.ERROR, 15,
            "Item at slot {0} is in wrong order");
    static Message m_EB0016 = new Message('R', 'B', MessageType.ERROR, 16,
            "Page {0} is marked for deallocation");
    static Message m_EB0017 = new Message('R', 'B', MessageType.ERROR, 17,
            "Page {0} has not been assigned a space map page");
    static Message m_EB0018 = new Message('R', 'B', MessageType.ERROR, 18,
            "Page {0} has a mismatch in keycount");
    static Message m_EB0019 = new Message('R', 'B', MessageType.ERROR, 19,
            "Deleted count on page {0} is > 0");
    static Message m_EB0020 = new Message('R', 'B', MessageType.ERROR, 20,
            "There is a mismatch between the node and the key type");

    static Message itemNotLeaf = new Message('R', 'B', MessageType.ERROR, 10,
            "Index item {0} is not setup as leaf");

    public BTreeIndexManagerImpl(Platform platform,
            ObjectRegistry objectFactory, LoggableFactory loggableFactory,
            FreeSpaceManager spaceMgr, BufferManager bufMgr,
            SlottedPageManager spMgr,
            TransactionalModuleRegistry moduleRegistry,
            LockAdaptor lockAdaptor, Properties p) {
        this.po = platform.getPlatformObjects(IndexManager.LOGGER_NAME);
        this.log = po.getLogger();
        this.exceptionHandler = po.getExceptionHandler();
        this.tracer = po.getTraceBuffer();
        this.objectFactory = objectFactory;
        this.loggableFactory = loggableFactory;
        this.spaceMgr = spaceMgr;
        this.bufmgr = bufMgr;
        this.spMgr = spMgr;
        this.lockAdaptor = lockAdaptor;
        moduleRegistry.registerModule(MODULE_ID, this);

        objectFactory.registerObjectFactory(TYPE_SPLIT_OPERATION,
                new SplitOperation.SplitOperationFactory(objectFactory));
        objectFactory.registerObjectFactory(TYPE_MERGE_OPERATION,
                new MergeOperation.MergeOperationFactory(objectFactory));
        objectFactory.registerObjectFactory(TYPE_LINK_OPERATION,
                new LinkOperation.LinkOperationFactory(objectFactory));
        objectFactory.registerObjectFactory(TYPE_UNLINK_OPERATION,
                new UnlinkOperation.UnlinkOperationFactory(objectFactory));
        objectFactory.registerObjectFactory(TYPE_REDISTRIBUTE_OPERATION,
                new RedistributeOperation.RedistributeOperationFactory(
                        objectFactory));
        objectFactory.registerObjectFactory(TYPE_NEWREDISTRIBUTE_OPERATION,
                new NewRedistributeOperation.NewRedistributeOperationFactory(
                        objectFactory));
        objectFactory
                .registerObjectFactory(
                        TYPE_INCREASETREEHEIGHT_OPERATION,
                        new IncreaseTreeHeightOperation.IncreaseTreeHeightOperationFactory(
                                objectFactory));
        objectFactory
                .registerObjectFactory(
                        TYPE_DECREASETREEHEIGHT_OPERATION,
                        new DecreaseTreeHeightOperation.DecreaseTreeHeightOperationFactory(
                                objectFactory));
        objectFactory.registerObjectFactory(TYPE_INSERT_OPERATION,
                new InsertOperation.InsertOperationFactory(objectFactory));
        objectFactory.registerObjectFactory(TYPE_UNDOINSERT_OPERATION,
                new UndoInsertOperation.UndoInsertOperationFactory(
                        objectFactory));
        objectFactory.registerObjectFactory(TYPE_DELETE_OPERATION,
                new DeleteOperation.DeleteOperationFactory(objectFactory));
        objectFactory.registerObjectFactory(TYPE_UNDODELETE_OPERATION,
                new UndoDeleteOperation.UndoDeleteOperationFactory(
                        objectFactory));
        objectFactory.registerObjectFactory(TYPE_LOADPAGE_OPERATION,
                new LoadPageOperation.LoadPageOperationFactory(objectFactory));

    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.tx.BaseTransactionalModule#redo(org.simpledbm.rss.api.pm.Page, org.simpledbm.rss.api.tx.Redoable)
     */
    public final void redo(Page page, Redoable loggable) {
        if (loggable instanceof SplitOperation) {
            redoSplitOperation(page, (SplitOperation) loggable);
        } else if (loggable instanceof MergeOperation) {
            redoMergeOperation(page, (MergeOperation) loggable);
        } else if (loggable instanceof LinkOperation) {
            redoLinkOperation(page, (LinkOperation) loggable);
        } else if (loggable instanceof UnlinkOperation) {
            redoUnlinkOperation(page, (UnlinkOperation) loggable);
        } else if (loggable instanceof RedistributeOperation) {
            redoRedistributeOperation(page, (RedistributeOperation) loggable);
        } else if (loggable instanceof NewRedistributeOperation) {
            redoNewRedistributeOperation(page,
                    (NewRedistributeOperation) loggable);
        } else if (loggable instanceof IncreaseTreeHeightOperation) {
            redoIncreaseTreeHeightOperation(page,
                    (IncreaseTreeHeightOperation) loggable);
        } else if (loggable instanceof DecreaseTreeHeightOperation) {
            redoDecreaseTreeHeightOperation(page,
                    (DecreaseTreeHeightOperation) loggable);
        } else if (loggable instanceof InsertOperation) {
            redoInsertOperation(page, (InsertOperation) loggable);
        } else if (loggable instanceof UndoInsertOperation) {
            redoUndoInsertOperation(page, (UndoInsertOperation) loggable);
        } else if (loggable instanceof DeleteOperation) {
            redoDeleteOperation(page, (DeleteOperation) loggable);
        } else if (loggable instanceof UndoDeleteOperation) {
            redoUndoDeleteOperation(page, (UndoDeleteOperation) loggable);
        } else if (loggable instanceof LoadPageOperation) {
            redoLoadPageOperation(page, (LoadPageOperation) loggable);
        }
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.tx.BaseTransactionalModule#undo(org.simpledbm.rss.api.tx.Transaction, org.simpledbm.rss.api.tx.Undoable)
     */
    public final void undo(Transaction trx, Undoable undoable) {
        if (undoable instanceof InsertOperation) {
            undoInsertOperation(trx, (InsertOperation) undoable);
        } else if (undoable instanceof DeleteOperation) {
            undoDeleteOperation(trx, (DeleteOperation) undoable);
        }
    }

    /**
     * Redo a LoadPageOperation. A LoadPageOperation is used to log the actions
     * of the XMLLoader which reads an XML file and generates a B-Tree. It is
     * also used when initializing a new BTree.
     * 
     * @see #createIndex(Transaction, String, int, int, int, int, boolean)
     * @see XMLLoader
     */
    void redoLoadPageOperation(Page page, LoadPageOperation loadPageOp) {
        /*
         * A LoadPageOperation is applied to two pages: the BTree page being initialised
         * and the Space Map page that contains the used/unused status for the BTree page.
         */
        if (page.getPageId().getPageNumber() == loadPageOp
                .getSpaceMapPageNumber()) {
            /*
             * Log record is being applied to space map page.
             */
            FreeSpaceMapPage smp = (FreeSpaceMapPage) page;
            // update space allocation data
            smp.setSpaceBits(loadPageOp.getPageId().getPageNumber(),
                    PAGE_SPACE_USED);
        } else if (page.getPageId().getPageNumber() == loadPageOp.getPageId()
                .getPageNumber()) {
            /*
             * Log record is being applied to BTree page.
             */
            SlottedPage r = (SlottedPage) page;
            BTreeNode.formatPage(r, loadPageOp.getKeyFactoryType(), loadPageOp
                    .getLocationFactoryType(), loadPageOp.isLeaf(), loadPageOp
                    .isUnique(), loadPageOp.getSpaceMapPageNumber());
            BTreeNode node = new BTreeNode(po,
                    loadPageOp.getIndexItemFactory(), r);
            int k = FIRST_KEY_POS; // 0 is position of header item
            for (IndexItem item : loadPageOp.items) {
                node.replace(k++, item);
            }
            node.header.keyCount = loadPageOp.items.size();
            node.header.leftSibling = loadPageOp.leftSibling;
            node.header.rightSibling = loadPageOp.rightSibling;
            node.updateHeader();

            node.dump();
        }
    }

    /**
     * Redo a page split operation.
     * 
     * @see BTreeImpl#doSplit(Transaction,
     *      org.simpledbm.rss.impl.im.btree.BTreeIndexManagerImpl.BTreeContext)
     * @see SplitOperation
     */
    void redoSplitOperation(Page page, SplitOperation splitOperation) {
        if (page.getPageId().equals(splitOperation.getPageId())) {
            // q is the page to be split
            tracer.event(0, page.getPageId().getContainerId(), page.getPageId()
                    .getPageNumber());
            SlottedPage q = (SlottedPage) page;
            BTreeNode leftSibling = new BTreeNode(po, splitOperation
                    .getIndexItemFactory(), q);
            leftSibling.header.rightSibling = splitOperation.newSiblingPageNumber;
            leftSibling.header.keyCount = splitOperation.newKeyCount;
            // get rid of the keys that will move to the
            // sibling page prior to updating the high key
            while (q.getNumberOfSlots() > leftSibling.header.keyCount + 1) {
                q.purge(q.getNumberOfSlots() - 1);
            }
            if (splitOperation.isLeaf()) {
                // update the high key
                leftSibling.replace(splitOperation.newKeyCount,
                        splitOperation.highKey);
            }
            leftSibling.updateHeader();
            if (Validating) {
                leftSibling.validate();
            }
            leftSibling.dump();
        } else {
            // r is the newly allocated right sibling of q
            tracer.event(1, page.getPageId().getContainerId(), page.getPageId()
                    .getPageNumber(), splitOperation.getPageId()
                    .getContainerId(), splitOperation.getPageId()
                    .getPageNumber());
            SlottedPage r = (SlottedPage) page;
            BTreeNode.formatPage(r, splitOperation.getKeyFactoryType(),
                    splitOperation.getLocationFactoryType(), splitOperation
                            .isLeaf(), splitOperation.isUnique(),
                    splitOperation.spaceMapPageNumber);
            BTreeNode newSiblingNode = new BTreeNode(po, splitOperation
                    .getIndexItemFactory(), r);
            newSiblingNode.header.leftSibling = splitOperation.getPageId()
                    .getPageNumber();
            newSiblingNode.header.rightSibling = splitOperation.rightSibling;
            int k = FIRST_KEY_POS; // 0 is position of header item
            for (IndexItem item : splitOperation.items) {
                newSiblingNode.replace(k++, item);
            }
            newSiblingNode.header.keyCount = splitOperation.items.size();
            newSiblingNode.updateHeader();
            if (Validating) {
                newSiblingNode.validate();
            }
            newSiblingNode.dump();
        }
    }

    /**
     * Redo a merge operation.
     * 
     * @see BTreeImpl#doMerge(Transaction,
     *      org.simpledbm.rss.impl.im.btree.BTreeIndexManagerImpl.BTreeContext)
     * @see MergeOperation
     */
    void redoMergeOperation(Page page, MergeOperation mergeOperation) {
        if (page.getPageId().getPageNumber() == mergeOperation.rightSiblingSpaceMapPage) {
            tracer.event(2, page.getPageId().getContainerId(), page.getPageId()
                    .getPageNumber(), mergeOperation.rightSibling);
            FreeSpaceMapPage smp = (FreeSpaceMapPage) page;
            // deallocate page by marking it unused
            smp.setSpaceBits(mergeOperation.rightSibling, PAGE_SPACE_UNUSED);
        } else if (page.getPageId().getPageNumber() == mergeOperation
                .getPageId().getPageNumber()) {
            // left sibling - this page will aborb contents of right sibling.
            tracer.event(3, page.getPageId().getContainerId(), page.getPageId()
                    .getPageNumber(), mergeOperation.rightSibling);
            SlottedPage q = (SlottedPage) page;
            BTreeNode leftSibling = new BTreeNode(po, mergeOperation
                    .getIndexItemFactory(), q);
            int k;
            /*
             * FIXME - in the code below, we should use the methods
             * provided by BTreeNode to manipulate items in the page.
             */
            if (leftSibling.isLeaf()) {
                // delete the high key
                k = leftSibling.header.keyCount;
                q.delete(leftSibling.header.keyCount);
            } else {
                k = leftSibling.header.keyCount + 1;
            }
            int j = k;
            for (IndexItem item : mergeOperation.items) {
                q.insertAt(k++, item, true);
            }
            leftSibling.header.keyCount += mergeOperation.items.size()
                    - (leftSibling.isLeaf() ? 1 : 0);
            leftSibling.header.rightSibling = mergeOperation.rightRightSibling;
            leftSibling.updateHeader();
            if (QuickValidate) {
                leftSibling.validateItemAt(j);
            }
            if (Validating) {
                leftSibling.validate();
            }
            leftSibling.dump();
        } else if (page.getPageId().getPageNumber() == mergeOperation.rightSibling) {
            // mark right sibling as deallocated
            tracer.event(4, page.getPageId().getContainerId(), page.getPageId()
                    .getPageNumber());
            SlottedPage p = (SlottedPage) page;
            short flags = p.getFlags();
            p.setFlags((short) (flags | BTreeNode.NODE_TREE_DEALLOCATED));
        }
    }

    /**
     * Redo a link operation
     * 
     * @see BTreeImpl#doLink(Transaction,
     *      org.simpledbm.rss.impl.im.btree.BTreeIndexManagerImpl.BTreeContext)
     * @see LinkOperation
     */
    void redoLinkOperation(Page page, LinkOperation linkOperation) {
        tracer.event(5, linkOperation.rightSibling, page.getPageId()
                .getContainerId(), page.getPageId().getPageNumber());
        SlottedPage p = (SlottedPage) page;
        BTreeNode parent = new BTreeNode(po, linkOperation
                .getIndexItemFactory(), p);
        int k = 0;
        k = parent.findPosition(linkOperation.leftSibling);
        if (k == -1 || k > parent.header.keyCount) {
            // child pointer not found - corrupt b-tree?
            exceptionHandler.unexpectedErrorThrow(getClass(),
                    "redoLinkOperation", new IndexException(
                            new MessageInstance(m_EB0001)));
        }
        IndexItem item = parent.getItem(k);

        // Change the index entry of left child to point to right child
        assert item.getChildPageNumber() == linkOperation.leftSibling;
        item.setChildPageNumber(linkOperation.rightSibling);
        p.insertAt(k, item, true);

        // Insert new entry for left child
        IndexItem u = linkOperation.leftChildHighKey;
        p.insertAt(k, u, false);
        parent.header.keyCount = parent.header.keyCount + 1;
        parent.updateHeader();
        if (QuickValidate) {
            parent.validateItemAt(k);
        }
        if (Validating) {
            parent.validate();
        }
        parent.dump();
    }

    /**
     * Redo an unlink operation.
     * 
     * @see BTreeImpl#doUnlink(Transaction, BTreeContext)
     * @see UnlinkOperation
     */
    void redoUnlinkOperation(Page page, UnlinkOperation unlinkOperation) {
        tracer.event(6, unlinkOperation.rightSibling, page.getPageId()
                .getContainerId(), page.getPageId().getPageNumber());
        SlottedPage p = (SlottedPage) page;
        BTreeNode parent = new BTreeNode(po, unlinkOperation
                .getIndexItemFactory(), p);
        int k = 0;
        k = parent.findPosition(unlinkOperation.leftSibling);
        if (k == -1 || k > parent.header.keyCount) {
            exceptionHandler.unexpectedErrorThrow(getClass(),
                    "redoUnlinkOperation", new IndexException(
                            new MessageInstance(m_EB0001)));
        }
        if (QuickValidate) {
            parent.validateItemAt(k);
        }
        p.purge(k);
        IndexItem item = parent.getItem(k);
        if (item.getChildPageNumber() == unlinkOperation.rightSibling) {
            item.setChildPageNumber(unlinkOperation.leftSibling);
            p.insertAt(k, item, true);
        } else {
            exceptionHandler.unexpectedErrorThrow(getClass(),
                    "redoUnlinkOperation", new IndexException(
                            new MessageInstance(m_EB0001)));
        }
        parent.header.keyCount = parent.header.keyCount - 1;
        parent.updateHeader();
        if (QuickValidate) {
            parent.validateItemAt(k);
        }
        if (Validating) {
            parent.validate();
        }
        parent.dump();
    }

    /**
     * Redo a distribute operation.
     * 
     * @see BTreeImpl#doRedistribute(Transaction, BTreeContext)
     * @see RedistributeOperation
     * @deprecated
     */
    void redoRedistributeOperation(Page page,
            RedistributeOperation redistributeOperation) {
        SlottedPage p = (SlottedPage) page;
        BTreeNode node = new BTreeNode(po, redistributeOperation
                .getIndexItemFactory(), p);
        if (page.getPageId().getPageNumber() == redistributeOperation.leftSibling) {
            // processing Q
            if (redistributeOperation.targetSibling == redistributeOperation.leftSibling) {
                // moving key left
                // the new key will become the high key
                // FIXME Test case
                tracer.event(7, page.getPageId().getContainerId(), page
                        .getPageId().getPageNumber());
                node.header.keyCount = node.header.keyCount + 1;
                node.updateHeader();
                p.insertAt(node.header.keyCount, redistributeOperation.key,
                        true);
                if (redistributeOperation.isLeaf()) {
                    p.insertAt(node.header.keyCount - 1,
                            redistributeOperation.key, true);
                }
            } else {
                // moving key right
                // delete current high key
                tracer.event(8, page.getPageId().getContainerId(), page
                        .getPageId().getPageNumber());
                p.purge(node.header.keyCount);
                node.header.keyCount = node.header.keyCount - 1;
                node.updateHeader();
                if (redistributeOperation.isLeaf()) {
                    // previous key becomes new high key
                    IndexItem prevKey = node.getItem(node.header.keyCount - 1);
                    p.insertAt(node.header.keyCount, prevKey, true);
                }
            }
        } else {
            // processing R
            if (redistributeOperation.targetSibling == redistributeOperation.leftSibling) {
                // moving key left
                // delete key from position 1
                tracer.event(9, page.getPageId().getContainerId(), page
                        .getPageId().getPageNumber());
                p.purge(FIRST_KEY_POS);
                node.header.keyCount = node.header.keyCount - 1;
                node.updateHeader();
            } else {
                // moving key right
                // insert new key at position 1
                tracer.event(10, page.getPageId().getContainerId(), page
                        .getPageId().getPageNumber());
                p.insertAt(FIRST_KEY_POS, redistributeOperation.key, false);
                node.header.keyCount = node.header.keyCount + 1;
                node.updateHeader();
            }
        }
        if (Validating) {
            node.validate();
        }
        node.dump();
    }

    /**
     * Redo a distribute keys operation. keys are distributed approximately
     * evenly across the two pages. This implementation takes the place of the
     * older implementation which move only one key between the pages.
     * 
     * @see BTreeImpl#doNewRedistribute(Transaction, BTreeContext)
     * @see NewRedistributeOperation
     */
    void redoNewRedistributeOperation(Page page,
            NewRedistributeOperation redistributeOperation) {
        SlottedPage p = (SlottedPage) page;
        BTreeNode node = new BTreeNode(po, redistributeOperation
                .getIndexItemFactory(), p);
        if (page.getPageId().getPageNumber() == redistributeOperation.leftSibling) {
            // processing Q (left sibling)
            if (redistributeOperation.targetSibling == redistributeOperation.leftSibling) {
                // moving keys left, so keys will be added to the left sibling
                // the last key of the added keys will become the new high key
                tracer.event(7, page.getPageId().getContainerId(), page
                        .getPageId().getPageNumber());
                // if leaf page, delete extra high key
                if (node.isLeaf()) {
                    p.purge(node.header.keyCount);
                    node.header.keyCount = node.header.keyCount - 1;
                }
                // now add the keys
                int k = 0;
                for (IndexItem item : redistributeOperation.items) {
                    node.header.keyCount = node.header.keyCount + 1;
                    p.insertAt(node.header.keyCount, item, true);
                    k++;
                }
                // if leaf, then add the extra high key
                if (node.isLeaf()) {
                    node.header.keyCount = node.header.keyCount + 1;
                    p.insertAt(node.header.keyCount,
                            redistributeOperation.items.getLast(), true);
                }
                node.updateHeader();
            } else {
                // moving keys right, therefore must delete from left sibling
                tracer.event(8, page.getPageId().getContainerId(), page
                        .getPageId().getPageNumber());
                // if leaf page, delete extra high key
                if (node.isLeaf()) {
                    p.purge(node.header.keyCount);
                    node.header.keyCount = node.header.keyCount - 1;
                }
                // delete the required number of keys from left sibling
                int n = redistributeOperation.items.size();
                while (n > 0) {
                    p.purge(node.header.keyCount);
                    node.header.keyCount = node.header.keyCount - 1;
                    n--;
                }
                // if leaf, make the last key the new high key
                if (node.isLeaf()) {
                    // last key becomes new high key
                    IndexItem lastKey = node.getItem(node.header.keyCount);
                    node.header.keyCount = node.header.keyCount + 1;
                    p.insertAt(node.header.keyCount, lastKey, true);
                }
                node.updateHeader();
            }
        } else {
            // processing R (right sibling)
            if (redistributeOperation.targetSibling == redistributeOperation.leftSibling) {
                // moving keys left, therefore delete from right sibling
                tracer.event(9, page.getPageId().getContainerId(), page
                        .getPageId().getPageNumber());
                int n = redistributeOperation.items.size();
                while (n > 0) {
                    p.purge(FIRST_KEY_POS);
                    node.header.keyCount = node.header.keyCount - 1;
                    n--;
                }
                node.updateHeader();
            } else {
                // moving keys right, therefore insert into right sibling
                // insert new keys starting at position 1
                tracer.event(10, page.getPageId().getContainerId(), page
                        .getPageId().getPageNumber());
                int k = FIRST_KEY_POS;
                for (IndexItem item : redistributeOperation.items) {
                    p.insertAt(k, item, false);
                    node.header.keyCount = node.header.keyCount + 1;
                    k++;
                }
                node.updateHeader();
            }
        }
        if (Validating) {
            node.validate();
        }
        node.dump();
    }

    /**
     * Redo an increase tree height operation.
     * 
     * @see BTreeImpl#doIncreaseTreeHeight(Transaction, BTreeContext)
     * @see IncreaseTreeHeightOperation
     */
    void redoIncreaseTreeHeightOperation(Page page,
            IncreaseTreeHeightOperation ithOperation) {
        SlottedPage p = (SlottedPage) page;
        BTreeNode node = null; // new BTreeNode(ithOperation);
        if (p.getPageId().equals(ithOperation.getPageId())) {
            tracer.event(11, page.getPageId().getContainerId(), page
                    .getPageId().getPageNumber(), ithOperation.leftSibling,
                    ithOperation.rightSibling);
            // root page
            // get rid of existing entries by reinitializing page
            int savedPageNumber = p.getSpaceMapPageNumber();
            BTreeNode.formatPage(p, ithOperation.getKeyFactoryType(),
                    ithOperation.getLocationFactoryType(), false, ithOperation
                            .isUnique(), savedPageNumber);
            node = new BTreeNode(po, ithOperation.getIndexItemFactory(), p);
            // insert new items
            node.insert(1, ithOperation.rootItems.get(0));
            node.insert(2, ithOperation.rootItems.get(1));
            node.header.keyCount = 2;
            node.updateHeader();
        } else if (p.getPageId().getPageNumber() == ithOperation.leftSibling) {
            tracer.event(12, page.getPageId().getContainerId(), page
                    .getPageId().getPageNumber(), ithOperation.rightSibling);
            // new left sibling
            BTreeNode.formatPage(p, ithOperation.getKeyFactoryType(),
                    ithOperation.getLocationFactoryType(), ithOperation
                            .isLeaf(), ithOperation.isUnique(),
                    ithOperation.spaceMapPageNumber);
            node = new BTreeNode(po, ithOperation.getIndexItemFactory(), p);
            int k = FIRST_KEY_POS; // 0 is position of header item
            for (IndexItem item : ithOperation.items) {
                node.replace(k++, item);
            }
            node.header.rightSibling = ithOperation.rightSibling;
            node.header.keyCount = ithOperation.items.size();
            node.updateHeader();
        }
        if (node != null) {
            if (Validating) {
                node.validate();
            }
            node.dump();
        }
    }

    /**
     * Decrease tree height when root page has only one child and that child
     * does not have a sibling.
     * 
     * @see BTreeImpl#doDecreaseTreeHeight(org.simpledbm.rss.api.tx.Transaction,
     *      org.simpledbm.rss.impl.im.btree.BTreeIndexManagerImpl.BTreeContext)
     * @see DecreaseTreeHeightOperation
     */
    void redoDecreaseTreeHeightOperation(Page page,
            DecreaseTreeHeightOperation dthOperation) {
        if (page.getPageId().getPageNumber() == dthOperation.childPageSpaceMap) {
            // This is not executed if the space map is updated
            // as a separate action. But we leave this code here in case we 
            // wish to update the space map as part of the same action.
            // FIXME TEST case
            FreeSpaceMapPage smp = (FreeSpaceMapPage) page;
            // deallocate
            smp.setSpaceBits(dthOperation.childPageNumber, PAGE_SPACE_UNUSED);
        } else if (page.getPageId().getPageNumber() == dthOperation.getPageId()
                .getPageNumber()) {
            tracer.event(13, page.getPageId().getContainerId(), page
                    .getPageId().getPageNumber(), dthOperation.childPageNumber);
            // root page 
            // delete contents and absorb contents of only child
            SlottedPage p = (SlottedPage) page;
            int savedPageNumber = p.getSpaceMapPageNumber();
            BTreeNode
                    .formatPage(p, dthOperation.getKeyFactoryType(),
                            dthOperation.getLocationFactoryType(), dthOperation
                                    .isLeaf(), dthOperation.isUnique(),
                            savedPageNumber);
            BTreeNode node = new BTreeNode(po, dthOperation
                    .getIndexItemFactory(), p);
            int k = FIRST_KEY_POS;
            // add the keys from child page
            for (IndexItem item : dthOperation.items) {
                p.insertAt(k++, item, true);
            }
            node.header.keyCount = dthOperation.items.size();
            node.updateHeader();
            if (Validating) {
                node.validate();
            }
            node.dump();
        } else if (page.getPageId().getPageNumber() == dthOperation.childPageNumber) {
            // mark child page as deallocated.
            tracer.event(14, page.getPageId().getContainerId(), page
                    .getPageId().getPageNumber());
            SlottedPage p = (SlottedPage) page;
            short flags = p.getFlags();
            p.setFlags((short) (flags | BTreeNode.NODE_TREE_DEALLOCATED));
        }
    }

    /**
     * Performs an insert operation on the leaf page.
     * 
     * @param page Page where the insert should take place
     * @param insertOp The log record containing details of the insert operation
     * @see BTreeImpl#doInsert(Transaction, IndexKey, Location)
     * @see BTreeImpl#doInsertTraverse(Transaction,
     *      org.simpledbm.rss.impl.im.btree.BTreeIndexManagerImpl.BTreeContext)
     */
    void redoInsertOperation(Page page, InsertOperation insertOp) {
        tracer.event(15, page.getPageId().getContainerId(), page.getPageId()
                .getPageNumber());
        SlottedPage p = (SlottedPage) page;
        BTreeNode node = new BTreeNode(po, insertOp.getIndexItemFactory(), p);
        SearchResult sr = node.search(insertOp.getItem());
        assert !sr.exactMatch;
        if (sr.k == SearchResult.KEY_OUT_OF_BOUNDS) {
            // The new key is greater than all keys in the node
            // Must still be <= highkey
            assert node.getHighKey().compareTo(insertOp.getItem()) >= 0;
            sr.k = node.header.keyCount;
        }
        node.insert(sr.k, insertOp.getItem());
        node.header.keyCount = node.header.keyCount + 1;
        node.updateHeader();
        if (QuickValidate) {
            node.validateItemAt(sr.k);
        }
        if (Validating) {
            node.validate();
        }
        node.dump();
    }

    /**
     * Undo an insert operation on a leaf page. The insert key will be deleted.
     * 
     * @param page The page where the insert should be undone.
     * @param undoInsertOp The log record containing information on the undo
     *            operation.
     */
    void redoUndoInsertOperation(Page page, UndoInsertOperation undoInsertOp) {
        tracer.event(16, page.getPageId().getContainerId(), page.getPageId()
                .getPageNumber());
        SlottedPage p = (SlottedPage) page;
        BTreeNode node = new BTreeNode(po, undoInsertOp.getIndexItemFactory(),
                p);
        if (QuickValidate) {
            assert node.getItem(undoInsertOp.getPosition()).equals(
                    undoInsertOp.getItem());
            node.validateItemAt(undoInsertOp.getPosition());
        }
        node.page.purge(undoInsertOp.getPosition());
        node.header.keyCount = node.header.keyCount - 1;
        node.updateHeader();
        if (Validating) {
            node.validate();
        }
        node.dump();
    }

    /**
     * Performs a logical undo of a key insert. Checks if the page that
     * originally contained the key is still the right page. If not, traverses
     * the tree to find the correct page.
     * 
     * @param trx The transaction handling this operation
     * @param insertOp The insert operation that will be undone.
     */
    void undoInsertOperation(Transaction trx, InsertOperation insertOp) {
        //		Undo-insert(T,P,k,m) { X-latch(P);
        //		if (P still contains r and will not underflow if r is deleted) { Q = P;
        //		} else { unlatch(P); update-mode-traverse(k,Q);
        //		upgrade-latch(Q);
        //		}delete r from Q;
        //		log(n, <T, undo-insert, Q, k, m>);
        //		Page-LSN(Q) = n; Undo-Next-LSN(T) = m; unlatch(Q);
        //		}

        BTreeContext bcursor = new BTreeContext(tracer);
        tracer.event(17, insertOp.getPageId().getContainerId(), insertOp
                .getPageId().getPageNumber());
        bcursor.setP(bufmgr.fixExclusive(insertOp.getPageId(), false, -1, 0));
        try {
            SlottedPage p = (SlottedPage) bcursor.getP().getPage();
            BTreeNode node = new BTreeNode(po, insertOp.getIndexItemFactory(),
                    p);
            SearchResult sr = null;
            boolean doSearch = false;
            /*
             * Page may have been deleted by the time the undo operation is executed.
             */
            if (!node.isLeaf() || node.isDeallocated()) {
                /*
                 * We need to traverse the tree to find the leaf page where the key
                 * now lives.
                 */
                tracer.event(123, insertOp.getPageId().getContainerId(),
                        insertOp.getPageId().getPageNumber());
                doSearch = true;
            } else {
                sr = node.search(insertOp.getItem());
                if (sr.exactMatch && node.header.keyCount > node.minimumKeys()) {
                    tracer.event(18, p.getPageId().getContainerId(), p
                            .getPageId().getPageNumber());
                    /*
                     * Page still contains the key and will not underflow if the key
                     * is deleted
                     */
                    doSearch = false;
                } else {
                    tracer.event(124, p.getPageId().getContainerId(), p
                            .getPageId().getPageNumber());
                    /*
                     * We need to traverse the tree to find the leaf page where the key
                     * now lives.
                     */
                    doSearch = true;
                }
            }
            if (doSearch) {
                /*
                 * We need to traverse the tree to find the leaf page where the key
                 * now lives.
                 */
                tracer.event(19);
                bcursor.unfixP();
                BTreeImpl btree = getBTreeImpl(insertOp.getPageId()
                        .getContainerId(), insertOp.getKeyFactoryType(),
                        insertOp.getLocationFactoryType(), insertOp.isUnique());
                bcursor.setSearchKey(insertOp.getItem());
                btree.updateModeTraverse(trx, bcursor);
                /* At this point p points to the leaf page where the key is present */
                assert bcursor.getP() != null;
                assert bcursor.getP().isLatchedForUpdate();
                tracer.event(20, bcursor.getP().getPage().getPageId()
                        .getContainerId(), bcursor.getP().getPage().getPageId()
                        .getPageNumber());
                bcursor.getP().upgradeUpdateLatch();
                p = (SlottedPage) bcursor.getP().getPage();
                node = new BTreeNode(po, insertOp.getIndexItemFactory(), p);
                assert node.isLeaf();
                assert !node.isDeallocated();

                sr = node.search(insertOp.getItem());
            }
            assert sr != null;
            assert sr.exactMatch;
            assert node.header.keyCount > node.minimumKeys();

            /*
             * Now we can remove the key the was inserted. First generate 
             * a Compensation Log Record.
             */
            UndoInsertOperation undoInsertOp = new UndoInsertOperation(
                    MODULE_ID, BTreeIndexManagerImpl.TYPE_UNDOINSERT_OPERATION,
                    insertOp);
            undoInsertOp.setPosition(sr.k);
            undoInsertOp.setUndoNextLsn(insertOp.getPrevTrxLsn());
            Lsn lsn = trx.logInsert(bcursor.getP().getPage(), undoInsertOp);
            redo(bcursor.getP().getPage(), undoInsertOp);
            bcursor.getP().setDirty(lsn);
        } finally {
            bcursor.unfixP();
        }

    }

    /**
     * Redo a delete operation on the leaf page.
     * 
     * @param page Page where the key is to be deleted from
     * @param deleteOp The log operation describing the delete
     */
    void redoDeleteOperation(Page page, DeleteOperation deleteOp) {
        tracer.event(21, page.getPageId().getContainerId(), page.getPageId()
                .getPageNumber());
        SlottedPage p = (SlottedPage) page;
        BTreeNode node = new BTreeNode(po, deleteOp.getIndexItemFactory(), p);
        SearchResult sr = node.search(deleteOp.getItem());
        assert sr.exactMatch;
        if (QuickValidate) {
            node.validateItemAt(sr.k);
        }
        node.purge(sr.k);
        node.header.keyCount = node.header.keyCount - 1;
        node.updateHeader();
        if (Validating) {
            node.validate();
        }
        node.dump();
    }

    /**
     * Redo an undo operation on a key delete on the specified leaf page.
     * 
     * @param page Page where the deleted key will be restored
     * @param undoDeleteOp The log operation describing the undo operation.
     */
    void redoUndoDeleteOperation(Page page, UndoDeleteOperation undoDeleteOp) {
        tracer.event(22, page.getPageId().getContainerId(), page.getPageId()
                .getPageNumber());
        SlottedPage p = (SlottedPage) page;
        BTreeNode node = new BTreeNode(po, undoDeleteOp.getIndexItemFactory(),
                p);
        node.insert(undoDeleteOp.getPosition(), undoDeleteOp.getItem());
        if (QuickValidate) {
            node.validateItemAt(undoDeleteOp.getPosition());
        }
        node.header.keyCount = node.header.keyCount + 1;
        node.updateHeader();
        if (Validating) {
            node.validate();
        }
        node.dump();
    }

    /**
     * Perform a logical undo operation. If the page containing the original key
     * is no longer the right page to perform an undo, then the BTree will be
     * traversed to locate the right page.
     * 
     * @param trx The transaction constrolling this operation
     * @param deleteOp The log record that describes the delete that will be
     *            undone.
     */
    void undoDeleteOperation(Transaction trx, DeleteOperation deleteOp) {
        //		X-latch(P);
        //		if (P still covers r and there is a room for r in P) { Q = P;
        //		} else { unlatch(P); update-mode-traverse(k,Q);
        //		if (Q is full) split(Q);
        //		upgrade-latch(Q);
        //		} insert r into Q;
        //		log(n, <T, undo-delete, Q, (k, x), m>);
        //		Page-LSN(Q) = n; Undo-Next-LSN(T) = m; unlatch(Q);

        BTreeContext bcursor = new BTreeContext(tracer);
        tracer.event(23, deleteOp.getPageId().getContainerId(), deleteOp
                .getPageId().getPageNumber());
        bcursor.setP(bufmgr.fixExclusive(deleteOp.getPageId(), false, -1, 0));
        try {
            SlottedPage p = (SlottedPage) bcursor.getP().getPage();
            BTreeNode node = new BTreeNode(po, deleteOp.getIndexItemFactory(),
                    p);
            /*
             * There is no easy way of knowing whether a page still covers r, since pages may
             * have been merged and split since the key was originally deleted. We take a cautious
             * approach and retraverse the tree if the page has been updated since it was 
             * originally modified. 
             */
            if ((p.getPageLsn() == deleteOp.getLsn() || (!node.isDeallocated()
                    && node.isLeaf() && node.covers(deleteOp.getItem())))
                    && node.canAccomodate(deleteOp.getItem())) {
                /*
                 * P sill covers r and there is room for r in P. 
                 */
                tracer.event(125, deleteOp.getPageId().getContainerId(),
                        deleteOp.getPageId().getPageNumber());
            } else {
                /*
                 * We need to traverse the tree to find the leaf page where the key
                 * now lives.
                 */
                tracer.event(24, deleteOp.getPageId().getContainerId(),
                        deleteOp.getPageId().getPageNumber());
                bcursor.unfixP();
                BTreeImpl btree = getBTreeImpl(deleteOp.getPageId()
                        .getContainerId(), deleteOp.getKeyFactoryType(),
                        deleteOp.getLocationFactoryType(), deleteOp.isUnique());
                bcursor.setSearchKey(deleteOp.getItem());
                btree.updateModeTraverse(trx, bcursor);
                /* At this point p points to the leaf page where the key should be inserted */
                assert bcursor.getP() != null;
                assert bcursor.getP().isLatchedForUpdate();
                p = (SlottedPage) bcursor.getP().getPage();
                node = new BTreeNode(po, deleteOp.getIndexItemFactory(), p);
                if (!node.canAccomodate(bcursor.searchKey)) {
                    tracer.event(126, p.getPageId().getContainerId(), p
                            .getPageId().getPageNumber());
                    bcursor.setQ(bcursor.removeP());
                    btree.doSplit(trx, bcursor);
                    bcursor.setP(bcursor.removeQ());
                }
                assert bcursor.getP().isLatchedForUpdate();
                tracer.event(25, bcursor.getP().getPage().getPageId()
                        .getContainerId(), bcursor.getP().getPage().getPageId()
                        .getPageNumber());
                bcursor.getP().upgradeUpdateLatch();
                p = (SlottedPage) bcursor.getP().getPage();
                node = new BTreeNode(po, deleteOp.getIndexItemFactory(), p);
                assert node.isLeaf();
                assert !node.isDeallocated();
            }
            SearchResult sr = node.search(deleteOp.getItem());
            assert !sr.exactMatch;
            if (sr.k == SearchResult.KEY_OUT_OF_BOUNDS) {
                // this is the rightmost key in this node
                assert node.getHighKey().compareTo(deleteOp.getItem()) >= 0;
                sr.k = node.header.keyCount;
            }
            assert sr.k != SearchResult.KEY_OUT_OF_BOUNDS;

            /*
             * Now we can reinsert the key that was deleted. First generate 
             * a Compensation Log Record.
             */
            UndoDeleteOperation undoDeleteOp = new UndoDeleteOperation(
                    MODULE_ID, BTreeIndexManagerImpl.TYPE_UNDODELETE_OPERATION,
                    deleteOp);
            undoDeleteOp.setPosition(sr.k);
            undoDeleteOp.setUndoNextLsn(deleteOp.getPrevTrxLsn());
            Lsn lsn = trx.logInsert(bcursor.getP().getPage(), undoDeleteOp);
            redo(bcursor.getP().getPage(), undoDeleteOp);
            bcursor.getP().setDirty(lsn);
        } finally {
            bcursor.unfixP();
        }
    }

    /**
     * Returns a BTree implementation.
     */
    final BTreeImpl getBTreeImpl(int containerId, int keyFactoryType,
            int locationFactoryType, boolean unique) {
        return new BTreeImpl(this, containerId, keyFactoryType,
                locationFactoryType, unique);
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.im.IndexManager#getIndex(int)
     */
    public final IndexContainer getIndex(int containerId) {
        /*
         * TODO - We could cache the index information here,
         * but we would need to ensure that if the index is deleted,
         * its information will be removed from the cache. Or maybe that
         * is not an issue because the system will never access a deleted
         * index?
         */
        int keyFactoryType = -1;
        int locationFactoryType = -1;
        boolean unique = false;

        PageId rootPageId = new PageId(containerId, ROOT_PAGE_NUMBER);
        BufferAccessBlock bab = bufmgr.fixShared(rootPageId, 0);
        try {
            /*
             * FIXME: This method has knowledge of the BTreeNode and
             * BTreeNodeHeader structures. Ideally this ought to be encapsulated
             * in BTreeNode
             */
            SlottedPage page = (SlottedPage) bab.getPage();
            unique = (page.getFlags() & BTreeNode.NODE_TREE_UNIQUE) != 0;
            BTreeNodeHeader header = (BTreeNodeHeader) page.get(HEADER_KEY_POS,
                    new BTreeNodeHeader.BTreeNodeHeaderStorabeFactory());
            keyFactoryType = header.getKeyFactoryType();
            locationFactoryType = header.getLocationFactoryType();
        } finally {
            bab.unfix();
        }
        return getBTreeImpl(containerId, keyFactoryType, locationFactoryType,
                unique);
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.im.IndexManager#getIndex(org.simpledbm.rss.api.tx.Transaction, int)
     */
    public IndexContainer getIndex(Transaction trx, int containerId) {
        lockIndexContainer(trx, containerId, LockMode.SHARED);
        return getIndex(containerId);
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.im.IndexManager#lockIndexContainer(org.simpledbm.rss.api.tx.Transaction, int, org.simpledbm.rss.api.locking.LockMode)
     */
    public void lockIndexContainer(Transaction trx, int containerId,
            LockMode mode) {
        trx.acquireLock(lockAdaptor.getLockableContainerId(containerId), mode,
                LockDuration.COMMIT_DURATION);
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.im.IndexManager#createIndex(org.simpledbm.rss.api.tx.Transaction, java.lang.String, int, int, int, int, boolean)
     */
    public final void createIndex(Transaction trx, String name,
            int containerId, int extentSize, int keyFactoryType,
            int locationFactoryType, boolean unique) {

        Savepoint sp = trx.createSavepoint(false);
        boolean success = false;
        try {

            lockIndexContainer(trx, containerId, LockMode.EXCLUSIVE);
            /*
             * Create the specified container
             */
            spaceMgr.createContainer(trx, name, containerId, 1, extentSize,
                    spMgr.getPageType());

            PageId pageid = new PageId(containerId, ROOT_PAGE_NUMBER);
            /*
             * Initialize the root page, and update space map page. 
             */
            LoadPageOperation loadPageOp = new LoadPageOperation(MODULE_ID,
                    TYPE_LOADPAGE_OPERATION, objectFactory, keyFactoryType,
                    locationFactoryType, true, unique);
            loadPageOp.setSpaceMapPageNumber(1);
            loadPageOp.leftSibling = -1;
            loadPageOp.rightSibling = -1;
            loadPageOp.setPageId(spMgr.getPageType(), pageid);

            IndexKey key = loadPageOp.getMaxIndexKey();
            Location location = loadPageOp.getNewLocation();
            loadPageOp.items.add(new IndexItem(key, location, -1, loadPageOp
                    .isLeaf(), loadPageOp.isUnique()));

            BufferAccessBlock bab = bufmgr.fixExclusive(pageid, false, -1, 0);
            try {
                PageId spaceMapPageId = new PageId(pageid.getContainerId(),
                        loadPageOp.getSpaceMapPageNumber());
                BufferAccessBlock smpBab = bufmgr.fixExclusive(spaceMapPageId,
                        false, -1, 0);
                try {
                    Lsn lsn = trx.logInsert(bab.getPage(), loadPageOp);
                    redo(bab.getPage(), loadPageOp);
                    bab.setDirty(lsn);
                    redo(smpBab.getPage(), loadPageOp);
                    smpBab.setDirty(lsn);
                } finally {
                    smpBab.unfix();
                }
            } finally {
                bab.unfix();
            }
            success = true;
        } finally {
            if (!success) {
                // FIXME TEST case
                trx.rollback(sp);
            }
        }
    }

    public static final class BTreeImpl implements IndexContainer {
        public final FreeSpaceCursor spaceCursor;
        final BTreeIndexManagerImpl btreeMgr;
        final int containerId;

        final int keyFactoryType;
        final int locationFactoryType;

        final IndexKeyFactory keyFactory;
        final LocationFactory locationFactory;

        boolean unique;

        final IndexItemFactory indexItemFactory;
        final PlatformObjects po;
        final TraceBuffer tracer;

        BTreeImpl(BTreeIndexManagerImpl btreeMgr, int containerId,
                int keyFactoryType, int locationFactoryType, boolean unique) {
            this.btreeMgr = btreeMgr;
            this.po = btreeMgr.po;
            this.tracer = btreeMgr.tracer;
            this.containerId = containerId;
            this.keyFactoryType = keyFactoryType;
            this.locationFactoryType = locationFactoryType;
            this.keyFactory = (IndexKeyFactory) btreeMgr.objectFactory
                    .getSingleton(keyFactoryType);
            this.locationFactory = (LocationFactory) btreeMgr.objectFactory
                    .getSingleton(locationFactoryType);
            this.unique = unique;
            this.indexItemFactory = new IndexItemFactory(keyFactory,
                    locationFactory, unique);
            spaceCursor = btreeMgr.spaceMgr.getSpaceCursor(containerId);
        }

        public final boolean isUnique() {
            return unique;
        }

        public IndexKeyFactory getIndexKeyFactory() {
            return keyFactory;
        }

        public LocationFactory getLocationFactory() {
            return locationFactory;
        }

        public final void setUnique(boolean unique) {
            this.unique = unique;
        }

        /**
         * Performs page split. The page to be split (bcursor.q) must be latched
         * in UPDATE mode prior to the call. After the split, the page
         * containing the search key will remain latched as bcursor.q.
         * <p>
         * This differs from the published algorithm in following ways: 1. It
         * uses nested top action. 2. Page allocation is logged as redo-undo. 3.
         * Space map page latch is released prior to any other exclusive latch.
         * 4. The split is logged as Compensation record, with undoNextLsn set
         * to the LSN prior to the page allocation log record. 5. Information
         * about space map page is stored in new page.
         * 
         * @see SplitOperation
         * @param trx Transaction managing the page split operation
         * @param bcursor bcursor.q must be the page that is to be split.
         */
        public final void doSplit(Transaction trx, BTreeContext bcursor) {

            assert bcursor.getQ().isLatchedForUpdate();

            final BTreeImpl btree = this;

            Lsn undoNextLsn = null;

            int newSiblingPageNumber = btree.spaceCursor
                    .findAndFixSpaceMapPageExclusively(new SpaceCheckerImpl());
            int spaceMapPageNumber = -1;
            try {
                if (newSiblingPageNumber == -1) {
                    tracer.event(26);
                    btree.btreeMgr.spaceMgr.extendContainer(trx,
                            btree.containerId);
                    undoNextLsn = trx.getLastLsn();
                    newSiblingPageNumber = btree.spaceCursor
                            .findAndFixSpaceMapPageExclusively(new SpaceCheckerImpl());
                    if (newSiblingPageNumber == -1) {
                        po.getExceptionHandler().unexpectedErrorThrow(
                                getClass(),
                                "doSplit",
                                new IndexException(
                                        new MessageInstance(m_EB0002)));
                    }
                }
                undoNextLsn = trx.getLastLsn();
                btree.spaceCursor.updateAndLogUndoably(trx,
                        newSiblingPageNumber, PAGE_SPACE_USED);
                spaceMapPageNumber = btree.spaceCursor.getCurrentSpaceMapPage()
                        .getPageId().getPageNumber();
            } finally {
                if (newSiblingPageNumber != -1) {
                    btree.spaceCursor.unfixCurrentSpaceMapPage();
                }
            }

            tracer.event(27, bcursor.getQ().getPage().getPageId()
                    .getContainerId(), bcursor.getQ().getPage().getPageId()
                    .getPageNumber());
            bcursor.getQ().upgradeUpdateLatch();
            assert bcursor.getQ().isLatchedExclusively();
            BTreeNode leftSiblingNode = new BTreeNode(po, indexItemFactory,
                    bcursor.getQ().getPage());

            PageId newSiblingPageId = new PageId(btree.containerId,
                    newSiblingPageNumber);
            SplitOperation splitOperation = new SplitOperation(
                    BTreeIndexManagerImpl.MODULE_ID,
                    BTreeIndexManagerImpl.TYPE_SPLIT_OPERATION,
                    btreeMgr.objectFactory, keyFactoryType,
                    locationFactoryType, leftSiblingNode.isLeaf(), isUnique());

            /*
             * The SplitOperation is a Compensation log record,
             * and its undoNextLsn is set to point to the log record
             * prior to the space map update log record. This allows
             * the space map update to be undone in case the system
             * crashes and the SplitOperation is lost. But if 
             * SplitOperation survives, the undo of the space map 
             * update is skipped during rollback. The SplitOperation 
             * is of course never undone.
             */
            splitOperation.setUndoNextLsn(undoNextLsn);
            splitOperation.newSiblingPageNumber = newSiblingPageNumber;
            splitOperation.rightSibling = leftSiblingNode.header.rightSibling;
            splitOperation.spaceMapPageNumber = spaceMapPageNumber;
            short medianKey = leftSiblingNode.getSplitKey();
            for (int k = medianKey; k <= leftSiblingNode.header.keyCount; k++) {
                splitOperation.items.add(leftSiblingNode.getItem(k));
            }
            splitOperation.highKey = leftSiblingNode.getItem(medianKey - 1);
            if (leftSiblingNode.isLeaf()) {
                splitOperation.newKeyCount = medianKey;
            } else {
                splitOperation.newKeyCount = (short) (medianKey - 1);
            }

            tracer.event(28, newSiblingPageId.getContainerId(),
                    newSiblingPageId.getPageNumber());
            bcursor.setR(btree.btreeMgr.bufmgr.fixExclusive(newSiblingPageId,
                    true, btree.btreeMgr.spMgr.getPageType(), 0));

            try {
                Lsn lsn = trx.logInsert(leftSiblingNode.page, splitOperation);

                btree.btreeMgr.redo(bcursor.getR().getPage(), splitOperation);
                bcursor.getR().setDirty(lsn);
                btree.btreeMgr.redo(leftSiblingNode.page, splitOperation);
                bcursor.getQ().setDirty(lsn);

                /* Check if Q covers the current search key value */
                int comp = splitOperation.highKey.compareTo(bcursor.searchKey);
                if (comp >= 0) {
                    // new key will stay in current page
                    tracer.event(29, bcursor.getQ().getPage().getPageId()
                            .getContainerId(), bcursor.getQ().getPage()
                            .getPageId().getPageNumber());
                    bcursor.getQ().downgradeExclusiveLatch();
                    assert bcursor.getQ().isLatchedForUpdate();
                    bcursor.unfixR();
                } else {
                    // new key will be in the right sibling
                    tracer.event(30, bcursor.getR().getPage().getPageId()
                            .getContainerId(), bcursor.getR().getPage()
                            .getPageId().getPageNumber());
                    bcursor.getR().downgradeExclusiveLatch();
                    assert bcursor.getR().isLatchedForUpdate();
                    bcursor.unfixQ();
                    bcursor.setQ(bcursor.removeR());
                }
                assert bcursor.getQ().isLatchedForUpdate();
            } finally {
                bcursor.unfixR();
            }
        }

        /**
         * Merges right sibling into left sibling. Right sibling must be an
         * indirect child. Both pages must be latched in UPDATE mode prior to
         * this call. After the merge, left sibling will remain latched as
         * bcursor.q.
         * <p>
         * This algorithm differs from published algorithm in its management of
         * space map update. In the interests of high concurrency, the space map
         * page update is handled as a separate redo only action.
         */
        public final void doMerge(Transaction trx, BTreeContext bcursor) {

            final BTreeImpl btree = this;

            assert bcursor.getR() != null;
            assert bcursor.getR().isLatchedForUpdate();

            assert bcursor.getQ() != null;
            assert bcursor.getQ().isLatchedForUpdate();

            tracer.event(31, bcursor.getQ().getPage().getPageId()
                    .getContainerId(), bcursor.getQ().getPage().getPageId()
                    .getPageNumber());
            bcursor.getQ().upgradeUpdateLatch();
            BTreeNode leftSiblingNode = new BTreeNode(po, indexItemFactory,
                    bcursor.getQ().getPage());

            tracer.event(32, bcursor.getR().getPage().getPageId()
                    .getContainerId(), bcursor.getR().getPage().getPageId()
                    .getPageNumber());
            bcursor.getR().upgradeUpdateLatch();
            BTreeNode rnode = new BTreeNode(po, indexItemFactory, bcursor
                    .getR().getPage());

            assert leftSiblingNode.header.rightSibling == rnode.page
                    .getPageId().getPageNumber();

            //			SlottedPage rpage = (SlottedPage) bcursor.getR().getPage();
            //			PageId spaceMapPageId = new PageId(btree.containerId, rpage.getSpaceMapPageNumber()); 

            //			BufferAccessBlock smpBab = btree.btreeMgr.bufmgr.fixExclusive(spaceMapPageId, false, "", 0);

            MergeOperation mergeOperation = new MergeOperation(
                    BTreeIndexManagerImpl.MODULE_ID,
                    BTreeIndexManagerImpl.TYPE_MERGE_OPERATION,
                    btreeMgr.objectFactory, keyFactoryType,
                    locationFactoryType, leftSiblingNode.isLeaf(), isUnique());
            mergeOperation.rightSibling = leftSiblingNode.header.rightSibling;
            mergeOperation.rightRightSibling = rnode.header.rightSibling;
            for (int k = FIRST_KEY_POS; k <= rnode.header.keyCount; k++) {
                mergeOperation.items.add(rnode.getItem(k));
            }
            mergeOperation.rightSiblingSpaceMapPage = rnode.page
                    .getSpaceMapPageNumber();

            try {
                Lsn lsn = trx.logInsert(leftSiblingNode.page, mergeOperation);
                btree.btreeMgr.redo(leftSiblingNode.page, mergeOperation);
                bcursor.getQ().setDirty(lsn);

                btree.btreeMgr.redo(rnode.page, mergeOperation);
                bcursor.getR().setDirty(lsn);

                //				btree.btreeMgr.redo(smpBab.getPage(), mergeOperation);
                //				smpBab.setDirty(lsn);
            } finally {
                //				smpBab.unfix();

                bcursor.unfixR();

                tracer.event(33, bcursor.getQ().getPage().getPageId()
                        .getContainerId(), bcursor.getQ().getPage().getPageId()
                        .getPageNumber());
                bcursor.getQ().downgradeExclusiveLatch();
            }
            assert bcursor.getQ().isLatchedForUpdate();

            /*
             * We log the space map operation as a separate discrete action.
             * If this log record does not survive a system crash, then the page
             * will end up appearing allocated. However the actual page will be
             * marked as deallocated, and hence can be reclaimed later on.
             * Note that this is different from the published algorithm but
             * is meant to provide greater concurrency.
             */
            btree.spaceCursor.fixSpaceMapPageExclusively(
                    mergeOperation.rightSiblingSpaceMapPage,
                    mergeOperation.rightSibling);
            try {
                btree.spaceCursor.updateAndLogRedoOnly(trx,
                        mergeOperation.rightSibling, 0);
            } finally {
                btree.spaceCursor.unfixCurrentSpaceMapPage();
            }
        }

        /**
         * Link the right sibling to the parent, when the right sibling is an
         * indirect child. Parent and left child must be latched in UPDATE mode
         * prior to invoking this method. Both will remain latched at the end of
         * the operation.
         * <p>
         * Note that this differs from published algorithm slightly:
         * 
         * <pre>
         * v = highkey of R
         * u = highkey of Q
         * Link(P, Q, R) {
         * 	upgrade-latch(P);
         * 	change the index record (v, Q.pageno) to (v, R.pageno);
         * 	insert the index record (u, Q.pageno) before (v, R.pageno);
         * 	lsn = log(&lt;unlink, P, Q.pageno, R.pageno&gt;);
         * 	P.pageLsn = lsn;
         * 	downgrade-latch(P);
         * }
         * </pre>
         */
        public final void doLink(Transaction trx, BTreeContext bcursor) {

            final BTreeImpl btree = this;

            assert bcursor.getP() != null;
            assert bcursor.getP().isLatchedForUpdate();

            assert bcursor.getQ() != null;
            assert bcursor.getQ().isLatchedForUpdate();

            tracer.event(34, bcursor.getP().getPage().getPageId()
                    .getContainerId(), bcursor.getP().getPage().getPageId()
                    .getPageNumber());
            bcursor.getP().upgradeUpdateLatch();
            try {
                BTreeNode parentNode = new BTreeNode(po, indexItemFactory,
                        bcursor.getP().getPage());
                BTreeNode lnode = new BTreeNode(po, indexItemFactory, bcursor
                        .getQ().getPage());
                SlottedPage lpage = (SlottedPage) bcursor.getQ().getPage();

                if (Validating) {
                    assert parentNode.findIndexItem(lnode.header.rightSibling) == null;
                }

                LinkOperation linkOperation = new LinkOperation(
                        BTreeIndexManagerImpl.MODULE_ID,
                        BTreeIndexManagerImpl.TYPE_LINK_OPERATION,
                        btreeMgr.objectFactory, keyFactoryType,
                        locationFactoryType, parentNode.isLeaf(), isUnique());

                linkOperation.leftSibling = lpage.getPageId().getPageNumber();
                linkOperation.rightSibling = lnode.header.rightSibling;
                IndexItem u = lnode.getHighKey();
                u.setChildPageNumber(linkOperation.leftSibling);
                u.setLeaf(false);
                linkOperation.leftChildHighKey = u;

                Lsn lsn = trx.logInsert(parentNode.page, linkOperation);
                btree.btreeMgr.redo(parentNode.page, linkOperation);
                bcursor.getP().setDirty(lsn);
            } finally {
                tracer.event(35, bcursor.getP().getPage().getPageId()
                        .getContainerId(), bcursor.getP().getPage().getPageId()
                        .getPageNumber());
                bcursor.getP().downgradeExclusiveLatch();
            }

            assert bcursor.getP() != null;
            assert bcursor.getP().isLatchedForUpdate();

            assert bcursor.getQ() != null;
            assert bcursor.getQ().isLatchedForUpdate();
        }

        /**
         * Unlink the right child from the parent. The page to the right of the
         * right child must not be an indirect child. This operation requires
         * parent, and the two child nodes to be latched in UPDATE mode prior to
         * invocation. At the end of the operation the parent is released.
         * <p>
         * Note that this differs from published algorithm slightly:
         * 
         * <pre>
         * v = highkey of R
         * u = highkey of Q
         * Unlink(P, Q, R) {
         * 	upgrade-latch(P);
         * 	delete the index record (u, Q.pageno);
         * 	change the index record (v, R.pageno) to (v, Q.pageno);
         * 	lsn = log(&lt;unlink, P, Q.pageno, R.pageno&gt;);
         * 	P.pageLsn = lsn;
         * 	unfix(P);
         * }
         * </pre>
         */
        public final void doUnlink(Transaction trx, BTreeContext bcursor) {

            final BTreeImpl btree = this;

            assert bcursor.getP() != null;
            assert bcursor.getP().isLatchedForUpdate();

            assert bcursor.getQ() != null;
            assert bcursor.getQ().isLatchedForUpdate();

            assert bcursor.getR() != null;
            assert bcursor.getR().isLatchedForUpdate();

            tracer.event(36, bcursor.getP().getPage().getPageId()
                    .getContainerId(), bcursor.getP().getPage().getPageId()
                    .getPageNumber());
            bcursor.getP().upgradeUpdateLatch();
            try {
                BTreeNode parentNode = new BTreeNode(po, indexItemFactory,
                        bcursor.getP().getPage());
                UnlinkOperation unlinkOperation = new UnlinkOperation(
                        BTreeIndexManagerImpl.MODULE_ID,
                        BTreeIndexManagerImpl.TYPE_UNLINK_OPERATION,
                        btreeMgr.objectFactory, keyFactoryType,
                        locationFactoryType, parentNode.isLeaf(), isUnique());

                unlinkOperation.leftSibling = bcursor.getQ().getPage()
                        .getPageId().getPageNumber();
                unlinkOperation.rightSibling = bcursor.getR().getPage()
                        .getPageId().getPageNumber();
                Lsn lsn = trx.logInsert(parentNode.page, unlinkOperation);
                btree.btreeMgr.redo(parentNode.page, unlinkOperation);
                bcursor.getP().setDirty(lsn);
            } finally {
                bcursor.unfixP();
            }

            assert bcursor.getR() != null;
            assert bcursor.getR().isLatchedForUpdate();

            assert bcursor.getQ() != null;
            assert bcursor.getQ().isLatchedForUpdate();
        }

        /**
         * Redistribute the keys between sibling nodes when the right child is
         * an indirect child of parent page. Both pages must be latched in
         * UPDATE mode prior to calling this method. At the end of this
         * operation, the child page that covers the search key will remain
         * latched in UPDATE mode.
         * <p>
         * Unlike the published algorithm we simply transfer one key from the
         * more densely populated page to the less populated page.
         * 
         * @param bcursor bcursor.q must point to left page, and bcursor.r to
         *            its right sibling
         * @deprecated
         */
        public final void doRedistribute(Transaction trx, BTreeContext bcursor) {

            final BTreeImpl btree = this;

            assert bcursor.getQ() != null;
            assert bcursor.getQ().isLatchedForUpdate();

            assert bcursor.getR() != null;
            assert bcursor.getR().isLatchedForUpdate();

            assert bcursor.searchKey != null;

            tracer.event(37, bcursor.getQ().getPage().getPageId()
                    .getContainerId(), bcursor.getQ().getPage().getPageId()
                    .getPageNumber());
            bcursor.getQ().upgradeUpdateLatch();
            BTreeNode leftSiblingNode = new BTreeNode(po, indexItemFactory,
                    bcursor.getQ().getPage());

            tracer.event(38, bcursor.getR().getPage().getPageId()
                    .getContainerId(), bcursor.getR().getPage().getPageId()
                    .getPageNumber());
            bcursor.getR().upgradeUpdateLatch();
            BTreeNode rightSiblingNode = new BTreeNode(po, indexItemFactory,
                    bcursor.getR().getPage());
            SlottedPage rpage = (SlottedPage) bcursor.getR().getPage();

            RedistributeOperation redistributeOperation = new RedistributeOperation(
                    BTreeIndexManagerImpl.MODULE_ID,
                    BTreeIndexManagerImpl.TYPE_REDISTRIBUTE_OPERATION,
                    btreeMgr.objectFactory, keyFactoryType,
                    locationFactoryType, leftSiblingNode.isLeaf(), isUnique());

            redistributeOperation.leftSibling = leftSiblingNode.page
                    .getPageId().getPageNumber();
            redistributeOperation.rightSibling = rpage.getPageId()
                    .getPageNumber();
            if (leftSiblingNode.page.getFreeSpace() > rpage.getFreeSpace()) {
                // key moving left
                redistributeOperation.key = rightSiblingNode
                        .getItem(FIRST_KEY_POS);
                redistributeOperation.targetSibling = redistributeOperation.leftSibling;
            } else {
                // key moving right
                redistributeOperation.key = leftSiblingNode.getLastKey();
                redistributeOperation.targetSibling = redistributeOperation.rightSibling;
            }

            try {
                Lsn lsn = trx.logInsert(leftSiblingNode.page,
                        redistributeOperation);

                btree.btreeMgr
                        .redo(leftSiblingNode.page, redistributeOperation);
                bcursor.getQ().setDirty(lsn);

                btree.btreeMgr.redo(rpage, redistributeOperation);
                bcursor.getR().setDirty(lsn);

                leftSiblingNode = new BTreeNode(po, indexItemFactory, bcursor
                        .getQ().getPage());
                /* Check if Q covers the current search key value */
                int comp = leftSiblingNode.getHighKey().compareTo(
                        bcursor.searchKey);
                if (comp >= 0) {
                    // new key will stay in current page
                    // FIXME TEST case
                    tracer.event(39, bcursor.getQ().getPage().getPageId()
                            .getContainerId(), bcursor.getQ().getPage()
                            .getPageId().getPageNumber());
                    bcursor.getQ().downgradeExclusiveLatch();
                    bcursor.unfixR();
                } else {
                    // new key will be in the right sibling
                    tracer.event(40, bcursor.getR().getPage().getPageId()
                            .getContainerId(), bcursor.getR().getPage()
                            .getPageId().getPageNumber());
                    bcursor.getR().downgradeExclusiveLatch();
                    bcursor.unfixQ();
                    bcursor.setQ(bcursor.removeR());
                }
                assert bcursor.getQ() != null;
                assert bcursor.getQ().isLatchedForUpdate();

                assert bcursor.getR() == null;
            } finally {
                bcursor.unfixR();
            }
        }

        /**
         * Redistribute the keys between sibling nodes when the right child is
         * an indirect child of parent page. Both pages must be latched in
         * UPDATE mode prior to calling this method. At the end of this
         * operation, the child page that covers the search key will remain
         * latched in UPDATE mode.
         * 
         * @param bcursor bcursor.q must point to left page, and bcursor.r to
         *            its right sibling
         */
        public final void doNewRedistribute(Transaction trx,
                BTreeContext bcursor) {

            final BTreeImpl btree = this;

            assert bcursor.getQ() != null;
            assert bcursor.getQ().isLatchedForUpdate();

            assert bcursor.getR() != null;
            assert bcursor.getR().isLatchedForUpdate();

            assert bcursor.searchKey != null;

            tracer.event(37, bcursor.getQ().getPage().getPageId()
                    .getContainerId(), bcursor.getQ().getPage().getPageId()
                    .getPageNumber());
            bcursor.getQ().upgradeUpdateLatch();
            BTreeNode leftSiblingNode = new BTreeNode(po, indexItemFactory,
                    bcursor.getQ().getPage());
            SlottedPage lpage = (SlottedPage) bcursor.getQ().getPage();

            tracer.event(38, bcursor.getR().getPage().getPageId()
                    .getContainerId(), bcursor.getR().getPage().getPageId()
                    .getPageNumber());
            bcursor.getR().upgradeUpdateLatch();
            BTreeNode rightSiblingNode = new BTreeNode(po, indexItemFactory,
                    bcursor.getR().getPage());
            SlottedPage rpage = (SlottedPage) bcursor.getR().getPage();

            NewRedistributeOperation redistributeOperation = new NewRedistributeOperation(
                    BTreeIndexManagerImpl.MODULE_ID,
                    BTreeIndexManagerImpl.TYPE_REDISTRIBUTE_OPERATION,
                    btreeMgr.objectFactory, keyFactoryType,
                    locationFactoryType, leftSiblingNode.isLeaf(), isUnique());

            redistributeOperation.leftSibling = leftSiblingNode.page
                    .getPageId().getPageNumber();
            redistributeOperation.rightSibling = rpage.getPageId()
                    .getPageNumber();
            int targetFreeSpace = (leftSiblingNode.page.getFreeSpace() + rpage
                    .getFreeSpace()) / 2;
            if (leftSiblingNode.page.getFreeSpace() > rpage.getFreeSpace()) {
                // key moving left
                int n = leftSiblingNode.page.getFreeSpace();
                assert n > targetFreeSpace;
                int k = FIRST_KEY_POS;
                while (n > targetFreeSpace) {
                    int len = rpage.getSlotLength(k);
                    if (n - len > targetFreeSpace) {
                        redistributeOperation.items.add(rightSiblingNode
                                .getItem(k));
                        n -= len;
                        k++;
                    } else {
                        break;
                    }
                }
                redistributeOperation.targetSibling = redistributeOperation.leftSibling;
            } else {
                // key moving right
                int n = rightSiblingNode.page.getFreeSpace();
                int k = leftSiblingNode.getKeyCount();
                while (n > targetFreeSpace) {
                    int len = lpage.getSlotLength(k);
                    if (n - len > targetFreeSpace) {
                        n -= len;
                        k--;
                    } else {
                        break;
                    }
                }
                for (; k <= leftSiblingNode.getKeyCount(); k++) {
                    redistributeOperation.items.add(leftSiblingNode.getItem(k));
                }
                redistributeOperation.targetSibling = redistributeOperation.rightSibling;
            }

            try {
                Lsn lsn = trx.logInsert(leftSiblingNode.page,
                        redistributeOperation);

                btree.btreeMgr
                        .redo(leftSiblingNode.page, redistributeOperation);
                bcursor.getQ().setDirty(lsn);

                btree.btreeMgr.redo(rpage, redistributeOperation);
                bcursor.getR().setDirty(lsn);

                leftSiblingNode = new BTreeNode(po, indexItemFactory, bcursor
                        .getQ().getPage());
                /* Check if Q covers the current search key value */
                int comp = leftSiblingNode.getHighKey().compareTo(
                        bcursor.searchKey);
                if (comp >= 0) {
                    // new key will stay in current page
                    // FIXME TEST case
                    tracer.event(39, bcursor.getQ().getPage().getPageId()
                            .getContainerId(), bcursor.getQ().getPage()
                            .getPageId().getPageNumber());
                    bcursor.getQ().downgradeExclusiveLatch();
                    bcursor.unfixR();
                } else {
                    // new key will be in the right sibling
                    tracer.event(40, bcursor.getR().getPage().getPageId()
                            .getContainerId(), bcursor.getR().getPage()
                            .getPageId().getPageNumber());
                    bcursor.getR().downgradeExclusiveLatch();
                    bcursor.unfixQ();
                    bcursor.setQ(bcursor.removeR());
                }
                assert bcursor.getQ() != null;
                assert bcursor.getQ().isLatchedForUpdate();

                assert bcursor.getR() == null;
            } finally {
                bcursor.unfixR();
            }
        }

        /**
         * Increase tree height when root page as a sibling page. The root page
         * and its sibling must be latched in UPDATE mode prior to calling this
         * method. After the operation is complete, the latch on the root page
         * is released, but one of the child pages (the one that covers the
         * search key) will be left latched in UPDATE mode.
         * <p>
         * The implementation differs from the published algorithm as follows:
         * <ol>
         * <li>
         * No need to format the new page, as this is taken care of in the space
         * management module. New pages are formatted as soon as they are
         * created.</li>
         * <li>
         * We use a nested top action to manage the entire action. This is to
         * improve concurrency, as it allows the space map page update to be
         * completed before any other page is latched exclusively. The SMO is
         * logged as a Compensation record and linked to the log record prior to
         * the space map update. This makes the SMO redoable, but the space map
         * update will be undone if the SMO log does not survive.</li>
         * </ol>
         * 
         * @param bcursor bcursor.q must point to root page, and bcursor.r to
         *            its right sibling
         */
        public final void doIncreaseTreeHeight(Transaction trx,
                BTreeContext bcursor) {

            final BTreeImpl btree = this;

            assert bcursor.getQ() != null;
            assert bcursor.getQ().isLatchedForUpdate();

            assert bcursor.getR() != null;
            assert bcursor.getR().isLatchedForUpdate();

            assert bcursor.getP() == null;

            assert bcursor.searchKey != null;

            Lsn undoNextLsn;

            // Allocate new page. 
            int newSiblingPageNumber = btree.spaceCursor
                    .findAndFixSpaceMapPageExclusively(new SpaceCheckerImpl());
            int spaceMapPageNumber = -1;
            try {
                if (newSiblingPageNumber == -1) {
                    // FIXME Test case
                    tracer.event(41);
                    btree.btreeMgr.spaceMgr.extendContainer(trx,
                            btree.containerId);
                    newSiblingPageNumber = btree.spaceCursor
                            .findAndFixSpaceMapPageExclusively(new SpaceCheckerImpl());
                    if (newSiblingPageNumber == -1) {
                        po.getExceptionHandler().unexpectedErrorThrow(
                                getClass(),
                                "doIncreaseTreeHeight",
                                new IndexException(
                                        new MessageInstance(m_EB0002)));
                    }
                }
                // Make a note of current lsn so that we can link the Compensation record to it.
                undoNextLsn = trx.getLastLsn();
                btree.spaceCursor.updateAndLogUndoably(trx,
                        newSiblingPageNumber, PAGE_SPACE_USED);
                spaceMapPageNumber = btree.spaceCursor.getCurrentSpaceMapPage()
                        .getPageId().getPageNumber();
            } finally {
                if (newSiblingPageNumber != -1) {
                    btree.spaceCursor.unfixCurrentSpaceMapPage();
                }
            }

            bcursor.setP(bcursor.removeQ());

            assert bcursor.getP().isLatchedForUpdate();
            tracer.event(42, bcursor.getP().getPage().getPageId()
                    .getContainerId(), bcursor.getP().getPage().getPageId()
                    .getPageNumber());
            bcursor.getP().upgradeUpdateLatch();
            BTreeNode rootNode = new BTreeNode(po, indexItemFactory, bcursor
                    .getP().getPage());

            IncreaseTreeHeightOperation ithOperation = new IncreaseTreeHeightOperation(
                    BTreeIndexManagerImpl.MODULE_ID,
                    BTreeIndexManagerImpl.TYPE_INCREASETREEHEIGHT_OPERATION,
                    btreeMgr.objectFactory, keyFactoryType,
                    locationFactoryType, rootNode.isLeaf(), isUnique());
            ithOperation.rightSibling = rootNode.header.rightSibling;
            ithOperation.setUndoNextLsn(undoNextLsn);
            ithOperation.leftSibling = newSiblingPageNumber;
            ithOperation.spaceMapPageNumber = spaceMapPageNumber;
            // New child page will inherit the root page leaf attribute

            for (int k = FIRST_KEY_POS; k <= rootNode.header.keyCount; k++) {
                ithOperation.items.add(rootNode.getItem(k));
            }
            IndexItem leftChildHighKey = rootNode
                    .getItem(rootNode.header.keyCount);
            leftChildHighKey.setLeaf(false);
            leftChildHighKey.setChildPageNumber(ithOperation.leftSibling);
            IndexItem rightChildHighKey = indexItemFactory.getInfiniteKey(
                    containerId, false);
            rightChildHighKey.setChildPageNumber(ithOperation.rightSibling);
            ithOperation.rootItems.add(leftChildHighKey);
            ithOperation.rootItems.add(rightChildHighKey);

            // Latch the new page exclusively
            PageId newChildPageId = new PageId(btree.containerId,
                    ithOperation.leftSibling);
            tracer.event(43, newChildPageId.getContainerId(), newChildPageId
                    .getPageNumber());
            bcursor.setQ(btree.btreeMgr.bufmgr.fixExclusive(newChildPageId,
                    true, btree.btreeMgr.spMgr.getPageType(), 0));
            try {
                Lsn lsn = trx.logInsert(rootNode.page, ithOperation);

                btree.btreeMgr.redo(rootNode.page, ithOperation);
                bcursor.getP().setDirty(lsn);

                btree.btreeMgr.redo(bcursor.getQ().getPage(), ithOperation);
                bcursor.getQ().setDirty(lsn);

                bcursor.unfixP();

                /*
                 * Check that Q covers the current search key.
                 */
                int comp = leftChildHighKey.compareTo(bcursor.searchKey);
                if (comp >= 0) {
                    // new key will stay in left child page
                    tracer.event(44, bcursor.getQ().getPage().getPageId()
                            .getContainerId(), bcursor.getQ().getPage()
                            .getPageId().getPageNumber());
                    bcursor.getQ().downgradeExclusiveLatch();
                    bcursor.unfixR();
                } else {
                    // new key will be in the right child page
                    tracer.event(45, bcursor.getR().getPage().getPageId()
                            .getContainerId(), bcursor.getR().getPage()
                            .getPageId().getPageNumber());
                    bcursor.unfixQ();
                    bcursor.setQ(bcursor.removeR());
                }
                assert bcursor.getQ().isLatchedForUpdate();
                assert bcursor.getP() == null;
                assert bcursor.getR() == null;
            } finally {
                // TODO is this robust enough?
                bcursor.unfixP();
                bcursor.unfixR();
            }
        }

        /**
         * Decrease tree height when root page has only one child and that child
         * does not have a right sibling. The root page and its child must be
         * latched in UPDATE mode prior to calling this method. At the end of
         * this operation, the root will remain latched in UPDATE mode.
         * <p>
         * Important note: To increase concurrency, we update the space map page
         * after the SMO as a separate redo only action. This improves
         * concurrency because we do not hold the space map page exclusively
         * during the SMO. However, it has the disadvantage that if the SMO
         * survives a system crash, and the log for the space map page updates
         * does not survive, then the page will remain allocated on the space
         * map, even though it is no longer in use. It is posible to identify
         * deallocated pages by checking the page flags for the bit BTreeNode.
         * 
         * @param bcursor bcursor.p must point to root page, and bcursor.q to
         *            only child
         */
        public final void doDecreaseTreeHeight(Transaction trx,
                BTreeContext bcursor) {

            final BTreeImpl btree = this;

            assert bcursor.getP() != null;
            assert bcursor.getP().isLatchedForUpdate();

            assert bcursor.getQ() != null;
            assert bcursor.getQ().isLatchedForUpdate();

            // root page
            tracer.event(46, bcursor.getP().getPage().getPageId()
                    .getContainerId(), bcursor.getP().getPage().getPageId()
                    .getPageNumber());
            bcursor.getP().upgradeUpdateLatch();
            BTreeNode rootNode = new BTreeNode(po, indexItemFactory, bcursor
                    .getP().getPage());

            // child page
            tracer.event(47, bcursor.getQ().getPage().getPageId()
                    .getContainerId(), bcursor.getQ().getPage().getPageId()
                    .getPageNumber());
            bcursor.getQ().upgradeUpdateLatch();
            BTreeNode childNode = new BTreeNode(po, indexItemFactory, bcursor
                    .getQ().getPage());

            // root will inherit the leaf status of child node
            DecreaseTreeHeightOperation dthOperation = new DecreaseTreeHeightOperation(
                    BTreeIndexManagerImpl.MODULE_ID,
                    BTreeIndexManagerImpl.TYPE_DECREASETREEHEIGHT_OPERATION,
                    btreeMgr.objectFactory, keyFactoryType,
                    locationFactoryType, childNode.isLeaf(), isUnique());

            for (int k = FIRST_KEY_POS; k <= childNode.header.keyCount; k++) {
                dthOperation.items.add(childNode.getItem(k));
            }
            dthOperation.childPageSpaceMap = childNode.page
                    .getSpaceMapPageNumber();
            dthOperation.childPageNumber = childNode.page.getPageId()
                    .getPageNumber();

            try {
                Lsn lsn = trx.logInsert(rootNode.page, dthOperation);

                btree.btreeMgr.redo(rootNode.page, dthOperation);
                bcursor.getP().setDirty(lsn);

                btree.btreeMgr.redo(bcursor.getQ().getPage(), dthOperation);
                bcursor.getQ().setDirty(lsn);
            } finally {
                // TODO Is this robust enough?
                bcursor.unfixQ();
                tracer.event(48, bcursor.getP().getPage().getPageId()
                        .getContainerId(), bcursor.getP().getPage().getPageId()
                        .getPageNumber());
                bcursor.getP().downgradeExclusiveLatch();
            }

            assert bcursor.getP() != null;
            assert bcursor.getP().isLatchedForUpdate();

            assert bcursor.getQ() == null;

            /*
             * We log the space map operation as a separate discrete action.
             * If this log record does not survive a system crash, then the page
             * will end up appearing allocated. However the actual page will be
             * marked as deallocated, and hence can be reclaimed later on.
             */
            btree.spaceCursor.fixSpaceMapPageExclusively(
                    dthOperation.childPageSpaceMap,
                    dthOperation.childPageNumber);
            try {
                btree.spaceCursor.updateAndLogRedoOnly(trx,
                        dthOperation.childPageNumber, 0);
            } finally {
                btree.spaceCursor.unfixCurrentSpaceMapPage();
            }
        }

        /**
         * Splits the parent node of current node Q. Parent must be latched in
         * UPDATE mode prior to the call. After the split is complete, the
         * parent node or its new sibling node, whichever covers the search key,
         * will be left latched as bcursor.p. Latches on child nodes will remain
         * unchanged.
         */
        final void doSplitParent(Transaction trx, BTreeContext bcursor) {
            /*
             * doSplit requires Q to point to page that is to be
             * split, so we need to point Q to P temporarily.
             */
            tracer.event(49, bcursor.getP().getPage().getPageId()
                    .getContainerId(), bcursor.getP().getPage().getPageId()
                    .getPageNumber());
            BufferAccessBlock savedQ = bcursor.removeQ(); // Save Q
            BufferAccessBlock savedR = bcursor.removeR(); // Save R
            bcursor.setQ(bcursor.removeP()); // Now Q is P
            try {
                doSplit(trx, bcursor);
            } finally {
                bcursor.setP(bcursor.removeQ());
                bcursor.setQ(savedQ);
                bcursor.setR(savedR);
            }
        }

        /**
         * Repairs underflow when an about-to-underflow child is encountered
         * during update mode traversal. Both the parent page (bcursor.p) and
         * its child page (bcursor.q) must be latched in UPDATE mode prior to
         * calling this method. When this method returns, the latch on the
         * parent page will have been released, and the child page that covers
         * the search key will remain latched in bcursor.q.
         * <p>
         * For this algorithm to work, an index page needs to have at least two
         * children who are linked the index page.
         */
        public final boolean doRepairPageUnderflow(Transaction trx,
                BTreeContext bcursor) {

            tracer.event(50);
            assert bcursor.getP() != null;
            assert bcursor.getP().isLatchedForUpdate();

            assert bcursor.getQ() != null;
            assert bcursor.getQ().isLatchedForUpdate();

            BTreeNode q = new BTreeNode(po, indexItemFactory, bcursor.getQ()
                    .getPage());
            BTreeNode p = new BTreeNode(po, indexItemFactory, bcursor.getP()
                    .getPage());
            BTreeNode r = null;

            int Q = q.page.getPageId().getPageNumber();

            IndexItem u = q.getHighKey();
            IndexItem highkeyP = p.getHighKey();
            /*
             * If the high key of Q is less than the high key of
             * P then Q is not the rightmost child of P.
             */
            if (u.compareTo(highkeyP) < 0) {
                tracer.event(51);
                /* Q is not the rightmost child of its parent P 
                 *
                 * There are three possibilities:
                 * a) R is an indirect child of P.
                 * b) R is a direct child of P, and has a sibling S that is an indirect child of P.
                 * c) R is a direct child of P and has a sibling S that is also a direct child of P.
                 */
                /* v = index record associated with Q in P */
                IndexItem v = p.findIndexItem(Q);
                assert v != null;
                assert v.getChildPageNumber() == q.getPage().getPageId()
                        .getPageNumber();

                /* R = rightsibling of Q */
                int R = q.header.rightSibling;
                assert R != -1;

                PageId rightPageId = new PageId(containerId, R);
                tracer.event(52, rightPageId.getContainerId(), rightPageId
                        .getPageNumber());
                bcursor.setR(btreeMgr.bufmgr.fixForUpdate(rightPageId, 0));
                r = new BTreeNode(po, indexItemFactory, bcursor.getR()
                        .getPage());
                /*
                 * If the high key of Q is less than the index key in P associated with Q,
                 * then R must be an indirect child of Q.
                 */
                if (u.compareTo(v) < 0) {
                    /*
                     * Possible validation - ensure that R pointer is not 
                     * in P.
                     */
                    /* a) R is an indirect child of P (case 13 in paper)
                     * This means that we can merge with R.
                     *                    P[v|w]
                     *           +----------+ +--------+
                     *           |                     |
                     *          Q[u]------>R[v]------>S[w]
                     */
                    tracer.event(53);
                    bcursor.unfixP();
                    if (q.canMergeWith(r)) {
                        tracer.event(54);
                        doMerge(trx, bcursor);
                    } else {
                        // FIXME TEST case
                        //                        doRedistribute(trx, bcursor);
                        doNewRedistribute(trx, bcursor);
                    }
                } else {
                    tracer.event(55);
                    /* b) or c) R is a direct child of P */
                    /* w = index record associated with R in P */
                    IndexItem w = p.findIndexItem(R);
                    assert w != null;
                    assert w.getChildPageNumber() == r.getPage().getPageId()
                            .getPageNumber();
                    /*
                     * S = right sibling of R.
                     */
                    int S = r.header.rightSibling;
                    /* v = highkey of R */
                    v = r.getHighKey();
                    /*
                     * If highkey of R (v) is less than the index key in P (w) associated with R,
                     * then R must have a right sibling S that is an indirect child of
                     * P. 
                     */
                    if (v.compareTo(w) < 0) {
                        assert S != -1;
                        if (Validating) {
                            assert p.findIndexItem(S) == null;
                        }
                        tracer.event(56);
                        /* b) R has a right sibling S that is indirect child of P (fig 14)
                         * 
                         *                    P[u|w]
                         *           +----------+ +
                         *           |            |
                         *          Q[u]-------->R[v]------>S[w]
                         * 
                         * We cannot unlink R from P until we have S linked to P.
                         * Therefore, we need to link S to P first.
                         * 
                         * If P cannot accomodate the index key v then it will need to be 
                         * split.
                         */
                        if (!p.canAccomodate(v)) {
                            tracer.event(57);
                            doSplitParent(trx, bcursor);
                            /*
                             * After the split, Q should still be a child of P, but
                             * R may have moved to the sibling of P.
                             */
                            assert bcursor.getP().isLatchedForUpdate();
                            assert bcursor.getQ().isLatchedForUpdate();
                            assert bcursor.getR().isLatchedForUpdate();

                            /*
                             * P might have changed as a result of the split of old P
                             */
                            p = new BTreeNode(po, indexItemFactory, bcursor
                                    .getP().getPage());
                            if (Validating) {
                                assert p.findIndexItem(Q) != null;
                            }
                            assert bcursor.getQ().getPage().getPageId()
                                    .getPageNumber() == Q;

                            if (p.findIndexItem(R) == null) {
                                tracer.event(58);
                                /* R is not a child of P anymore.
                                 * We need to restart the algorithm
                                 */
                                // FIXME TEST case
                                bcursor.unfixR();
                                return true;
                            }
                            assert bcursor.getR().getPage().getPageId()
                                    .getPageNumber() == R;
                        }
                        /*
                         * We need to link S to P. Since our cursor is currently
                         * positioned Q, we need to temporarily move right to R,
                         * in order to do the link.
                         */
                        BufferAccessBlock savedQ = bcursor.removeQ(); // Save Q
                        bcursor.setQ(bcursor.removeR()); // Now Q is R
                        try {
                            tracer.event(59);
                            doLink(trx, bcursor); // Link S to P
                        } finally {
                            bcursor.setR(bcursor.removeQ()); // Restore R
                            bcursor.setQ(savedQ); // Restore Q
                        }
                        assert bcursor.getP().isLatchedForUpdate();
                        assert bcursor.getQ().isLatchedForUpdate();

                        /*
                         * In the paper the unlink of R and merge of Q and R is here,
                         * but we do this in the common section below.
                         */
                        p = new BTreeNode(po, indexItemFactory, bcursor.getP()
                                .getPage());
                        assert p.findIndexItem(S) != null;
                    }

                    p = new BTreeNode(po, indexItemFactory, bcursor.getP()
                            .getPage());
                    assert p.findIndexItem(Q) != null;
                    assert p.findIndexItem(R) != null;

                    /*
                     * At this point any sibling of R (ie, S) is
                     * guaranteed to be linked to parent P. So we can now
                     * unlink R from P to allow merging of Q and R.
                     */
                    tracer.event(60);
                    doUnlink(trx, bcursor); // Now we can unlink R from P
                    /*
                     * Merge Q and R
                     */
                    q = new BTreeNode(po, indexItemFactory, bcursor.getQ()
                            .getPage());
                    r = new BTreeNode(po, indexItemFactory, bcursor.getR()
                            .getPage());
                    if (q.canMergeWith(r)) {
                        tracer.event(61);
                        doMerge(trx, bcursor);
                    } else {
                        // FIXME TEST case
                        //                        doRedistribute(trx, bcursor);
                        doNewRedistribute(trx, bcursor);
                    }
                }
            } else {
                tracer.event(62);
                assert u.compareTo(highkeyP) == 0;
                /* Q is the rightmost child of its parent P as Q.highkey = P.highkey.
                 * There are two possibilities.
                 * The leftsibling L of Q is 
                 * a) a direct child of P
                 * b) an indirect child of P.
                 */
                /* Find node L to the left of Q
                 * Note that since every page must have at least 2 items,
                 * we are guaranteed to find L.
                 */
                IndexItem v = p.findPrevIndexItem(Q);
                assert v != null;
                int L = v.getChildPageNumber();
                /*
                 * Since our cursor is positioned on Q, we need to move left.
                 * But to do that we need to unlatch Q first. 
                 */
                bcursor.unfixQ();
                /* Now L becomes Q */
                PageId l_pageId = new PageId(containerId, L);
                tracer.event(63, l_pageId.getContainerId(), l_pageId
                        .getPageNumber());
                bcursor.setQ(btreeMgr.bufmgr.fixForUpdate(l_pageId, 0));
                q = new BTreeNode(po, indexItemFactory, bcursor.getQ()
                        .getPage());
                /* The node to the right of L is N */
                /* This may or may not be Q depending upon whether N is
                 * an indirect child of P.
                 */
                int N = q.header.rightSibling;
                assert N != -1;
                PageId n_pageId = new PageId(containerId, N);
                tracer.event(64, n_pageId.getContainerId(), n_pageId
                        .getPageNumber());
                bcursor.setR(btreeMgr.bufmgr.fixForUpdate(n_pageId, 0));
                r = new BTreeNode(po, indexItemFactory, bcursor.getR()
                        .getPage());

                /*
                 * Get highkey of L (u) and compare with the index key associated with L (v) in
                 * page P. 
                 */
                u = q.getHighKey();
                if (u.compareTo(v) == 0) {
                    /* 
                     * Fig 17.
                     * L is direct child of P 
                     * and the right sibling of L is Q
                     * 
                     *                   P[u|v|w]
                     *           +---------+ + +--------+
                     *           |           |          |
                     *         ?[u]------->L[v]------->Q[w]
                     *  
                     */
                    tracer.event(65);
                    assert q.header.rightSibling == Q;
                    assert N == Q;
                    /*
                     * remember that Q holds L and R holds N (N == original Q). 
                     */
                    if (!r.isAboutToUnderflow()) {
                        tracer.event(66);
                        /* Q is no longer about to overflow (remember R holds original Q!) */
                        // FIXME Test case
                        bcursor.unfixP();
                        bcursor.unfixQ();
                        bcursor.setQ(bcursor.removeR());
                    } else {
                        /* In order to merge Q with its left sibling L, we need
                         * to unlink Q from its parent first.
                         * Remember that bcursor.q is positioned on L.
                         */
                        /* unlink Q from P */
                        tracer.event(67);
                        doUnlink(trx, bcursor);
                        /* 
                         * Fig 18.
                         * After about to underflow rightmost child Q is unlinked
                         * from its parent.
                         * 
                         *                   P[u|w]
                         *           +---------+ +
                         *           |           |
                         *         ?[u]------->L[v]------->Q[w]
                         *  
                         */
                        tracer.event(68);
                        q = new BTreeNode(po, indexItemFactory, bcursor.getQ()
                                .getPage());
                        r = new BTreeNode(po, indexItemFactory, bcursor.getR()
                                .getPage());

                        if (q.canMergeWith(r)) {
                            /* merge L and Q */
                            tracer.event(69);
                            doMerge(trx, bcursor);
                        } else {
                            // FIXME Test case
                            //                            doRedistribute(trx, bcursor);
                            doNewRedistribute(trx, bcursor);
                        }
                        /* Q = L (already true) */
                        // Issue 70: looks like incorrect assertion as redistribute keys can leave the bcursor.Q pointing to either L or Q
                        // Maybe did not encounter this before because the doRistribute() wasn't getting called?
                        // FIXME once this is verified need to remove the assertion or replace it with a correct one.
                        //                        assert bcursor.getQ().getPage().getPageId().getPageNumber() == L;
                    }
                } else {
                    /*
                     *  Fig 19 in paper.
                     * The left sibling L of Q has a right sibling N
                     * that is an indirect child of P. Q is the right
                     * sibling of N. 
                     * 
                     *                    P[v|w]
                     *           +----------+ +--------+
                     *           |                     |
                     *          L[u]------>N[v]------>Q[w]
                     * 
                     * 
                     * To merge Q with N, we first need to link
                     * N to parent page P, then unlink Q from P.
                     */
                    tracer.event(70);
                    if (!p.canAccomodate(u)) {
                        tracer.event(71);
                        doSplitParent(trx, bcursor);
                        /*
                         * Since Q was the rightmost child of P,
                         * even after the split Q and its left sibling L must 
                         * belong to P (because a minimum of 2 items must be
                         * present in a page).
                         */
                        p = new BTreeNode(po, indexItemFactory, bcursor.getP()
                                .getPage());
                        assert p.findIndexItem(Q) != null;
                        assert p.findIndexItem(L) != null;
                    }
                    /* link N to parent P */
                    tracer.event(72);
                    doLink(trx, bcursor);
                    /* unlatch L */
                    bcursor.unfixQ();
                    /* N becomes bcursor.q */
                    bcursor.setQ(bcursor.removeR());
                    q = new BTreeNode(po, indexItemFactory, bcursor.getQ()
                            .getPage());
                    /* latch Q again, which now becomes bcursor.r */
                    assert Q == q.header.rightSibling;
                    PageId q_pageId = new PageId(containerId, Q);
                    tracer.event(73, q_pageId.getContainerId(), q_pageId
                            .getPageNumber());
                    bcursor.setR(btreeMgr.bufmgr.fixForUpdate(q_pageId, 0));
                    r = new BTreeNode(po, indexItemFactory, bcursor.getR()
                            .getPage());
                    if (!r.isAboutToUnderflow()) {
                        /* Q is no longer about to underflow */
                        // FIXME Test case
                        tracer.event(74);
                        bcursor.unfixP();
                        bcursor.unfixQ();
                        bcursor.setQ(bcursor.removeR());

                    } else {
                        /* unlink Q from parent P (note that bcursor.r points to Q) */
                        tracer.event(75);
                        doUnlink(trx, bcursor);
                        /* here bcursor.q is N and bcursor.r is Q */
                        q = new BTreeNode(po, indexItemFactory, bcursor.getQ()
                                .getPage());
                        r = new BTreeNode(po, indexItemFactory, bcursor.getR()
                                .getPage());
                        if (q.canMergeWith(r)) {
                            /* merge N and Q */
                            tracer.event(76);
                            doMerge(trx, bcursor);
                        } else {
                            //                            doRedistribute(trx, bcursor);
                            doNewRedistribute(trx, bcursor);
                        }
                    }
                }
            }
            assert bcursor.getP() == null;
            assert bcursor.getQ() != null;
            assert bcursor.getQ().isLatchedForUpdate();
            return false;
        }

        /**
         * Repairs underflow when an about-to-underflow child is encountered
         * during update mode traversal. Both the parent page (bcursor.p) and
         * its child page (bcursor.q) must be latched in UPDATE mode prior to
         * calling this method. When this method returns, the latch on the
         * parent page will have been released, and the child page that covers
         * the search key will remain latched in bcursor.q.
         * <p>
         * For this algorithm to work, an index page needs to have at least two
         * children who are linked the index page.
         */
        public final void repairPageUnderflow(Transaction trx,
                BTreeContext bcursor) {
            boolean tryAgain = doRepairPageUnderflow(trx, bcursor);
            while (tryAgain) {
                // FIXME Test case
                tracer.event(77);
                tryAgain = doRepairPageUnderflow(trx, bcursor);
            }
        }

        /**
         * Walks down the tree using UPDATE mode latching. On the way down,
         * pages may be split or merged to ensure that the tree maintains its
         * balance. bcursor.searchKey represents the key being searched for.
         * When this returns bcursor.p will point to the page that contains or
         * should contain the search key. This page will be latched in UPDATE
         * mode.
         * <p>
         * This traversal mode is used for inserts and deletes. Corresponds to
         * Update-mode-traverse in btree paper.
         */
        public final void updateModeTraverse(Transaction trx,
                BTreeContext bcursor) {
            /*
             * Fix root page
             */
            PageId rootPageId = new PageId(containerId,
                    BTreeIndexManagerImpl.ROOT_PAGE_NUMBER);
            tracer.event(78, rootPageId.getContainerId(), rootPageId
                    .getPageNumber());
            bcursor.setP(btreeMgr.bufmgr.fixForUpdate(rootPageId, 0));
            BTreeNode p = new BTreeNode(po, indexItemFactory, bcursor.getP()
                    .getPage());
            if (p.header.rightSibling != -1) {
                /* 
                 * Root page has a right sibling. This means that the root page
                 * was split at some point, but the parent has not yet been
                 * created.
                 * A new child page will be allocated and the root will become
                 * the parent of this new child, and its right sibling. 
                 */
                PageId r_pageId = new PageId(containerId, p.header.rightSibling);
                bcursor.setQ(bcursor.removeP());
                tracer.event(79, r_pageId.getContainerId(), r_pageId
                        .getPageNumber());
                bcursor.setR(btreeMgr.bufmgr.fixForUpdate(r_pageId, 0));
                doIncreaseTreeHeight(trx, bcursor);
                bcursor.setP(bcursor.removeQ());
                p = new BTreeNode(po, indexItemFactory, bcursor.getP()
                        .getPage());
            }
            if (p.isLeaf()) {
                return;
            }
            int childPageNumber = p.findChildPage(bcursor.searchKey);
            PageId q_pageId = new PageId(containerId, childPageNumber);
            tracer.event(80, q_pageId.getContainerId(), q_pageId
                    .getPageNumber());
            bcursor.setQ(btreeMgr.bufmgr.fixForUpdate(q_pageId, 0));
            BTreeNode q = new BTreeNode(po, indexItemFactory, bcursor.getQ()
                    .getPage());
            boolean childPageLatched = true;
            if (p.isRoot() && p.header.keyCount == 1
                    && q.header.rightSibling == -1) {
                /* Q is only child of P and Q has no right sibling */
                /*
                 * Root is underflown as it has only one child and this child does not
                 * have a right sibling. Decrease the height of the tree by
                 * merging the child page into the root.
                 */
                tracer.event(81);
                doDecreaseTreeHeight(trx, bcursor);
                childPageLatched = false;
            }
            p = new BTreeNode(po, indexItemFactory, bcursor.getP().getPage());
            while (!p.isLeaf()) {
                if (!childPageLatched) {
                    /*
                     * BUG in published algorithm - need to avoid latching
                     * Q if already latched.
                     */
                    childPageNumber = p.findChildPage(bcursor.searchKey);
                    q_pageId = new PageId(containerId, childPageNumber);
                    tracer.event(82, q_pageId.getContainerId(), q_pageId
                            .getPageNumber());
                    bcursor.setQ(btreeMgr.bufmgr.fixForUpdate(q_pageId, 0));
                    q = new BTreeNode(po, indexItemFactory, bcursor.getQ()
                            .getPage());
                    /*
                     * No need to set childPageLatch as this is only needed first time round.
                     */
                } else {
                    childPageLatched = false;
                }
                if (q.isAboutToUnderflow()) {
                    tracer.event(83, q.getPage().getPageId().getContainerId(),
                            q.getPage().getPageId().getPageNumber());
                    repairPageUnderflow(trx, bcursor);
                    bcursor.setP(bcursor.removeQ());
                } else {
                    IndexItem v = p.findIndexItem(q.page.getPageId()
                            .getPageNumber());
                    IndexItem u = q.getHighKey();
                    if (u.compareTo(v) < 0) {
                        /* Q has a right sibling page R which is an
                         * indirect child of P. Also handles the case where R is
                         * the right most page.
                         * Fig 5 in the paper.
                         *                    P[v|w]
                         *           +----------+ +--------+
                         *           |                     |
                         *          Q[u]------>R[v]------>S[w]
                         */
                        tracer.event(84);
                        if (!p.canAccomodate(u)) {
                            tracer.event(85);
                            doSplitParent(trx, bcursor);
                        }
                        tracer.event(86);
                        doLink(trx, bcursor);
                    }
                    assert bcursor.getP().isLatchedForUpdate();
                    assert bcursor.getQ().isLatchedForUpdate();
                    q = new BTreeNode(po, indexItemFactory, bcursor.getQ()
                            .getPage());
                    if (q.getHighKey().compareTo(bcursor.searchKey) >= 0) {
                        /* Q covers search key */
                        tracer.event(87);
                        bcursor.unfixP();
                        bcursor.setP(bcursor.removeQ());
                    } else {
                        /* move right */
                        int rightsibling = q.header.rightSibling;
                        bcursor.unfixP();
                        assert q.header.rightSibling != -1;
                        PageId r_pageId = new PageId(containerId, rightsibling);
                        tracer.event(88, r_pageId.getContainerId(), r_pageId
                                .getPageNumber());
                        bcursor.setP(btreeMgr.bufmgr.fixForUpdate(r_pageId, 0));
                        bcursor.unfixQ();
                    }
                }
                p = new BTreeNode(po, indexItemFactory, bcursor.getP()
                        .getPage());
            }
        }

        /**
         * Walks down the tree acquiring shared latches on the way.
         * bcursor.searchKey represents the key being searched for. When this
         * returns bcursor.p will point to the leaf page that should contain the
         * search key. bcursor.p will be left in shared latch. Corresponds to
         * read-mode-traverse() in btree paper.
         */
        public final void readModeTraverse(BTreeContext bcursor) {

            if (btreeMgr.log.isDebugEnabled()) {
                btreeMgr.log.debug(getClass(), "readModeTraverse",
                        "SIMPLEDBM-DEBUG: Read mode traverse for search key = "
                                + bcursor.getSearchKey());
            }

            /*
             * Fix root page
             */
            PageId rootPageId = new PageId(containerId,
                    BTreeIndexManagerImpl.ROOT_PAGE_NUMBER);
            tracer.event(89, rootPageId.getContainerId(), rootPageId
                    .getPageNumber());
            bcursor.setP(btreeMgr.bufmgr.fixShared(rootPageId, 0));
            BTreeNode p = new BTreeNode(po, indexItemFactory, bcursor.getP()
                    .getPage());

            do {
                IndexItem v = p.getHighKey();
                if (v.compareTo(bcursor.getSearchKey()) < 0) {
                    // Move right as the search key is greater than the highkey
                    // FIXME TEST case
                    int rightsibling = p.header.rightSibling;
                    PageId r_pageId = new PageId(containerId, rightsibling);
                    tracer.event(90, r_pageId.getContainerId(), r_pageId
                            .getPageNumber());
                    bcursor.setQ(btreeMgr.bufmgr.fixShared(r_pageId, 0));
                    bcursor.unfixP();
                    bcursor.setP(bcursor.removeQ());
                    p = new BTreeNode(po, indexItemFactory, bcursor.getP()
                            .getPage());
                    assert p.getHighKey().compareTo(bcursor.getSearchKey()) >= 0;
                }
                if (!p.isLeaf()) {
                    // find the child page and move down
                    int childPageNumber = p.findChildPage(bcursor.searchKey);
                    PageId c_pageId = new PageId(containerId, childPageNumber);
                    tracer.event(91, c_pageId.getContainerId(), c_pageId
                            .getPageNumber());
                    bcursor.setQ(btreeMgr.bufmgr.fixShared(c_pageId, 0));
                    bcursor.unfixP();
                    bcursor.setP(bcursor.removeQ());
                    p = new BTreeNode(po, indexItemFactory, bcursor.getP()
                            .getPage());
                }
            } while (!p.isLeaf());
        }

        /**
         * Traverses a BTree down to the leaf level, and prepares the leaf page
         * for inserting the new key. bcursor.p must hold the root node in
         * update mode latch when this is called. When this returns bcursor.p
         * will point to the page where the insert should take place, and will
         * be exclusively latched.
         * 
         * @return SearchResult containing information about where to insert the
         *         new key
         */
        public final SearchResult doInsertTraverse(Transaction trx,
                BTreeContext bcursor) {
            updateModeTraverse(trx, bcursor);
            /* At this point p points to the leaf page where the key is to be inserted */
            assert bcursor.getP() != null;
            assert bcursor.getP().isLatchedForUpdate();
            BTreeNode node = new BTreeNode(po, indexItemFactory, bcursor.getP()
                    .getPage());
            assert node.isLeaf();
            assert !node.isDeallocated();
            assert node.getHighKey().compareTo(bcursor.searchKey) >= 0;
            if (!node.canAccomodate(bcursor.searchKey)) {
                tracer.event(92, bcursor.getP().getPage().getPageId()
                        .getContainerId(), bcursor.getP().getPage().getPageId()
                        .getPageNumber());
                bcursor.setQ(bcursor.removeP());
                doSplit(trx, bcursor);
                bcursor.setP(bcursor.removeQ());
            }
            tracer.event(93, bcursor.getP().getPage().getPageId()
                    .getContainerId(), bcursor.getP().getPage().getPageId()
                    .getPageNumber());
            bcursor.getP().upgradeUpdateLatch();
            node = new BTreeNode(po, indexItemFactory, bcursor.getP().getPage());
            SearchResult sr = node.search(bcursor.searchKey);
            return sr;
        }

        /**
         * Obtain a lock on the next key. Mode and duration are specified by the
         * caller. bcursor.P must contain the current page, latched exclusively.
         * If the lock on next key is successful, the page will be left latched.
         * Else, the latch on the page will be released.
         * 
         * @return True if insert can proceed, false if lock could not be
         *         obtained on next key.
         */
        public final boolean doNextKeyLock(Transaction trx,
                BTreeContext bcursor, int nextPageNumber, int nextk,
                LockMode mode, LockDuration duration) {

            assert bcursor.getP() != null;
            assert bcursor.getP().isLatchedExclusively();

            SlottedPage nextPage = null;
            IndexItem nextItem = null;
            Lsn nextPageLsn = null;
            // Make a note of current page and page Lsn
            int currentPageNumber = bcursor.getP().getPage().getPageId()
                    .getPageNumber();
            Lsn currentPageLsn = bcursor.getP().getPage().getPageLsn();
            bcursor.setNextKeyLocation(null);
            if (nextPageNumber != -1) {
                // next key is in the right sibling page
                PageId r_pageId = new PageId(containerId, nextPageNumber);
                tracer.event(94, r_pageId.getContainerId(), r_pageId
                        .getPageNumber());
                bcursor.setR(btreeMgr.bufmgr.fixShared(r_pageId, 0));
                nextPage = (SlottedPage) bcursor.getR().getPage();
                BTreeNode nextNode = new BTreeNode(po, indexItemFactory,
                        nextPage);
                nextItem = nextNode.getItem(nextk);
                nextPageLsn = nextPage.getPageLsn();
            } else {
                // next key is in the current page
                BTreeNode nextNode = new BTreeNode(po, indexItemFactory,
                        bcursor.getP().getPage());
                nextPageLsn = bcursor.getP().getPage().getPageLsn();
                if (nextk == -1) {
                    nextItem = nextNode.getHighKey(); // represents infinity
                } else {
                    nextItem = nextNode.getItem(nextk);
                }
            }
            try {
                tracer.event(95, nextItem.getLocation().getContainerId(),
                        nextItem.getLocation().getX(), nextItem.getLocation()
                                .getY());
                trx.acquireLockNowait(nextItem.getLocation(), mode, duration);
                bcursor.setNextKeyLocation(nextItem.getLocation());
                /*
                 * Instant duration lock succeeded. We can proceed with the insert.
                 */
                return true;
            } catch (LockException e) {

                if (btreeMgr.log.isDebugEnabled()) {
                    btreeMgr.log.debug(getClass(), "doNextKeyLock",
                            "SIMPLEDBM-DEBUG: Failed to acquire NOWAIT " + mode
                                    + " lock on " + nextItem.getLocation());
                }

                /*
                 * Someone else has inserted or deleted a key in the same key range.
                 * We need to unlatch all pages and unconditionally wait for a lock on
                 * the next key.
                 */
                bcursor.unfixP();
                bcursor.unfixR();

                // Delay for testing
                // FIXME At present we use a crude mechanism to decide whether to introduce an artificial delay
                if (Thread.currentThread().getName().equals(
                        "TestingInsertRestartDueToKeyRangeModification")) {
                    try {
                        Thread.sleep(2 * 1000);
                    } catch (InterruptedException e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }
                }

                /*
                 * Wait unconditionally for the other transaction to finish
                 */
                // trx.acquireLock(nextItem.getLocation(), mode, LockDuration.INSTANT_DURATION);
                // Above looks like an error as lock duration is hard coded???
                // FIXME What if this lock attempt fails due to deadlock - must ensure proper cleanup.
                tracer.event(96, nextItem.getLocation().getContainerId(),
                        nextItem.getLocation().getX(), nextItem.getLocation()
                                .getY());
                trx.acquireLock(nextItem.getLocation(), mode, duration);
                bcursor.setNextKeyLocation(nextItem.getLocation());
                /*
                 * Now we have obtained the lock.
                 * We can continue from where we were if nothing has changed in the meantime
                 */
                PageId c_pageId = new PageId(containerId, currentPageNumber);
                tracer.event(97, c_pageId.getContainerId(), c_pageId
                        .getPageNumber());
                bcursor.setP(btreeMgr.bufmgr.fixExclusive(c_pageId, false, -1,
                        0));
                if (nextPageNumber != -1) {
                    PageId n_pageId = new PageId(containerId, nextPageNumber);
                    tracer.event(98, n_pageId.getContainerId(), n_pageId
                            .getPageNumber());
                    bcursor.setR(btreeMgr.bufmgr.fixShared(n_pageId, 0));
                }
                if (currentPageLsn.compareTo(bcursor.getP().getPage()
                        .getPageLsn()) == 0) {
                    if (nextPageNumber == -1
                            || nextPageLsn.compareTo(bcursor.getR().getPage()
                                    .getPageLsn()) == 0) {
                        tracer.event(99);
                        /*
                         * Nothing has changed, so we can carry on as before.
                         */
                        bcursor.unfixR();
                        return true;
                    }
                } else {

                    /*
                     * We could avoid a rescan of the tree by checking that the next key
                     * previously identified hasn't changed. For now, we just give up and restart
                     * the insert.
                     */
                    bcursor.unfixR();
                    bcursor.unfixP();
                }
            }
            /*
             * Restart insert/delete
             */
            tracer.event(100);
            tracer.event(101, bcursor.getNextKeyLocation().getContainerId(),
                    bcursor.getNextKeyLocation().getX(), bcursor
                            .getNextKeyLocation().getY());
            trx.releaseLock(bcursor.getNextKeyLocation());
            bcursor.setNextKeyLocation(null);
            return false;
        }

        /**
         * @see #insert(Transaction, IndexKey, Location)
         */
        public final boolean doInsert(Transaction trx, IndexKey key,
                Location location) {

            BTreeContext bcursor = new BTreeContext(tracer);
            bcursor.searchKey = new IndexItem(key, location, -1, true,
                    isUnique());

            try {
                /*
                 * Traverse to leaf page
                 */
                SearchResult sr = doInsertTraverse(trx, bcursor);

                int nextKeyPage = -1;
                int nextk = -1;

                if (sr.k == SearchResult.KEY_OUT_OF_BOUNDS) {
                    /* next key is in the next page or it is
                     * INFINITY if this is the rightmost page.
                     */
                    BTreeNode node = new BTreeNode(po, indexItemFactory,
                            bcursor.getP().getPage());
                    nextKeyPage = node.header.rightSibling;
                    if (nextKeyPage != -1) {
                        nextk = FIRST_KEY_POS; // first key of next page
                        tracer.event(102);
                    } else {
                        tracer.event(103);
                    }
                } else {
                    /*
                     * We are positioned on a key that is either equal to
                     * searchkey or greater.
                     */
                    if (sr.exactMatch) {
                        Savepoint sp = trx.createSavepoint(false);
                        boolean needRollback = false;
                        Location loc = sr.item.getLocation();
                        LockDuration duration = (trx.getIsolationMode() == IsolationMode.CURSOR_STABILITY || trx
                                .getIsolationMode() == IsolationMode.READ_COMMITTED) ? LockDuration.MANUAL_DURATION
                                : LockDuration.COMMIT_DURATION;
                        /*
                         * Oops - possibly a unique constraint violation
                         */
                        try {
                            try {
                                tracer.event(104, loc.getContainerId(), loc
                                        .getX(), loc.getY());
                                trx.acquireLockNowait(loc, LockMode.SHARED,
                                        duration);
                            } catch (LockException e) {
                                // FIXME Test case
                                if (btreeMgr.log.isDebugEnabled()) {
                                    btreeMgr.log.debug(getClass(),
                                            "doInsert",
                                            "SIMPLEDBM-DEBUG: Failed to acquire NOWAIT "
                                                    + LockMode.SHARED
                                                    + " lock on " + loc);
                                }
                                /*
                                 * Failed to acquire conditional lock. 
                                 * Need to unlatch page and retry unconditionally. 
                                 */
                                bcursor.unfixP();
                                tracer.event(105, loc.getContainerId(), loc
                                        .getX(), loc.getY());
                                trx.acquireLock(loc, LockMode.SHARED, duration);
                                /*
                                 * We have obtained the lock. We need to double check that the key
                                 * still exists.
                                 */
                                PageId rootPageId = new PageId(containerId,
                                        BTreeIndexManagerImpl.ROOT_PAGE_NUMBER);
                                tracer.event(106);
                                tracer.event(107, rootPageId.getContainerId(),
                                        rootPageId.getPageNumber());
                                bcursor.setP(btreeMgr.bufmgr.fixForUpdate(
                                        rootPageId, 0));
                                sr = doInsertTraverse(trx, bcursor);
                            }
                            if (sr.exactMatch
                                    && sr.item.getLocation().equals(loc)) {
                                tracer.event(108);
                                /*
                                 * Mohan says that for RR we need a commit duration lock, but
                                 * for CS, maybe we should release the lock here??
                                 */
                                if (trx.getIsolationMode() == IsolationMode.CURSOR_STABILITY
                                        || trx.getIsolationMode() == IsolationMode.READ_COMMITTED) {
                                    tracer.event(109, loc.getContainerId(), loc
                                            .getX(), loc.getY());
                                    trx.releaseLock(loc);
                                }
                                //                                exceptionHandler.warnAndThrow(getClass().getName(), "doInsert", 
                                //                                		new UniqueConstraintViolationException(
                                //                                    mcat.getMessage("WB0003", key, location)));
                                throw new UniqueConstraintViolationException(
                                        new MessageInstance(m_WB0003, key,
                                                location, sr.item.getKey(),
                                                sr.item.getLocation()));

                            }
                            /*
                             * Key has been deleted or has been rolled back in the meantime
                             */
                            tracer.event(110);
                            needRollback = true;
                            /*
                             * Start again from the beginning
                             */
                            return false;
                        } finally {
                            if (needRollback) {
                                trx.rollback(sp);
                            }
                        }
                    } else {
                        /*
                         * We are positioned on a key greater than the search key.
                         */
                        nextk = sr.k;
                    }
                }
                /*
                 * Try to obtain instant lock on next key.
                 */
                // if (!doNextKeyLock(trx, bcursor, nextKeyPage, nextk, LockMode.EXCLUSIVE, LockDuration.INSTANT_DURATION)) {
                /* 
                 * It seems erroneous to obtain an instant_duration lock on next key,
                 * as until the key has been inserted, it does not safeguard the key range.
                 * We need to obtain the lock and then release it after the key has been inserted into the
                 * page.
                 * A potential optimisation would be to request conditional instant duration lock while the
                 * pages are latched, but manual duration lock if the pages are unlatched due to
                 * conditional lock not being available. Assumption is that while the page is latched,
                 * another transaction cannot obtain a lock on next key. I think however that this
                 * won't work because of data locking - therefore for now we take a conservative approach.
                 */
                if (!doNextKeyLock(trx, bcursor, nextKeyPage, nextk,
                        LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION)) {
                    return false;
                }
                /*
                 * We can finally insert the key and be done with!
                 */
                InsertOperation insertOp = new InsertOperation(
                        BTreeIndexManagerImpl.MODULE_ID,
                        BTreeIndexManagerImpl.TYPE_INSERT_OPERATION,
                        btreeMgr.objectFactory, keyFactoryType,
                        locationFactoryType, true, isUnique());

                insertOp.setItem(bcursor.searchKey);
                try {
                    Lsn lsn = trx.logInsert(bcursor.getP().getPage(), insertOp);
                    btreeMgr.redo(bcursor.getP().getPage(), insertOp);
                    bcursor.getP().setDirty(lsn);
                } finally {
                    bcursor.unfixP();
                }

                /*
                 * We can now release the lock on the next key.
                 */
                tracer.event(111,
                        bcursor.getNextKeyLocation().getContainerId(), bcursor
                                .getNextKeyLocation().getX(), bcursor
                                .getNextKeyLocation().getY());
                trx.releaseLock(bcursor.getNextKeyLocation());
            } finally {
                bcursor.setNextKeyLocation(null);
                bcursor.unfixAll();
            }

            return true;
        }

        /* (non-Javadoc)
         * @see org.simpledbm.rss.bt.BTree#insert(org.simpledbm.rss.tm.Transaction, org.simpledbm.rss.bt.IndexKey, org.simpledbm.rss.bt.Location)
         */
        public final void insert(Transaction trx, IndexKey key,
                Location location) {
            if (key.equals(indexItemFactory.getMaxIndexKey(containerId))) {
                po.getExceptionHandler().errorThrow(getClass(),
                        "insert",
                        new IndexException(new MessageInstance(m_EB0013)));
            }
            tracer.event(112);
            Savepoint savepoint = trx.createSavepoint(false);
            boolean success = false;
            try {
                success = doInsert(trx, key, location);
                while (!success) {
                    success = doInsert(trx, key, location);
                }
            } finally {
                if (!success) {
                    trx.rollback(savepoint);
                }
            }
        }

        /**
         * @see #delete(Transaction, IndexKey, Location)
         */
        public final boolean doDelete(Transaction trx, IndexKey key,
                Location location) {

            BTreeContext bcursor = new BTreeContext(tracer);
            bcursor.searchKey = new IndexItem(key, location, -1, true,
                    isUnique());

            try {
                /*
                 * Traverse to leaf page
                 */
                updateModeTraverse(trx, bcursor);
                assert bcursor.getP() != null;
                assert bcursor.getP().isLatchedForUpdate();

                /* 
                 * At this point p points to the leaf page where the key is to be deleted 
                 */
                bcursor.getP().upgradeUpdateLatch();
                BTreeNode node = new BTreeNode(po, indexItemFactory, bcursor
                        .getP().getPage());
                assert node.isLeaf();
                assert !node.isDeallocated();
                assert node.getHighKey().compareTo(bcursor.searchKey) >= 0;

                SearchResult sr = node.search(bcursor.searchKey);
                if (!sr.exactMatch) {
                    // key not found?? something is wrong
                    po.getExceptionHandler().unexpectedErrorThrow(
                            getClass(),
                            "doDelete",
                            new IndexException(new MessageInstance(m_EB0004,
                                    bcursor.searchKey.toString())));
                }

                int nextKeyPage = -1;
                int nextk = -1;

                if (sr.k == node.getKeyCount()) {
                    // this is the last key on the page
                    nextKeyPage = node.header.rightSibling;
                    if (nextKeyPage != -1) {
                        nextk = FIRST_KEY_POS;
                    }
                } else {
                    nextk = sr.k + 1;
                }

                /*
                 * Try to obtain commit duration lock on next key.
                 */
                if (!doNextKeyLock(trx, bcursor, nextKeyPage, nextk,
                        LockMode.EXCLUSIVE, LockDuration.COMMIT_DURATION)) {
                    return false;
                }
                /*
                 * Now we can delete the key and be done with!
                 */
                DeleteOperation deleteOp = new DeleteOperation(
                        BTreeIndexManagerImpl.MODULE_ID,
                        BTreeIndexManagerImpl.TYPE_DELETE_OPERATION,
                        btreeMgr.objectFactory, keyFactoryType,
                        locationFactoryType, true, isUnique());
                deleteOp.setItem(bcursor.searchKey);

                try {
                    Lsn lsn = trx.logInsert(bcursor.getP().getPage(), deleteOp);
                    btreeMgr.redo(bcursor.getP().getPage(), deleteOp);
                    bcursor.getP().setDirty(lsn);
                } finally {
                    bcursor.unfixP();
                }
            } finally {
                bcursor.unfixAll();
            }

            return true;
        }

        /**
         * Delete specified key and location combination. It is an error if the
         * key is not found. It is assumed that location is already locked in
         * exclusive mode. At the end of this operation, the next key will be
         * locked in exclusive mode, and the lock on location may be released.
         */
        public final void delete(Transaction trx, IndexKey key,
                Location location) {
            if (key.equals(indexItemFactory.getMaxIndexKey(containerId))) {
                po.getExceptionHandler().errorThrow(getClass(),
                        "delete",
                        new IndexException(new MessageInstance(m_EB0013)));
            }
            tracer.event(113);
            Savepoint savepoint = trx.createSavepoint(false);
            boolean success = false;
            try {
                success = doDelete(trx, key, location);
                while (!success) {
                    success = doDelete(trx, key, location);
                }
            } finally {
                if (!success) {
                    trx.rollback(savepoint);
                }
            }
        }

        /**
         * Searches for {@link IndexCursorImpl#currentKey} in the leaf node, and
         * positions the cursor on the current key or the next higher key.
         * Handles both fetch() and fetchNext() calls.
         * <p>
         * Cursor may hit EOF as a result.
         */
        final SearchResult doSearchAtLeafLevel(IndexCursorImpl icursor) {
            BTreeNode node = new BTreeNode(po, indexItemFactory,
                    icursor.bcursor.getP().getPage());
            icursor.setEof(false);
            SearchResult sr = null;
            if (icursor.scanMode == IndexCursorImpl.SCAN_FETCH_GREATER
                    && node.getPage().getPageLsn().equals(icursor.pageLsn)) {
                /*
                 * If this is a call to fetchNext, check if we can avoid searching the node
                 * If page LSN hasn't changed we can used cached values.
                 */
                tracer.event(129, node.getPage().getPageId().getContainerId(),
                        node.getPage().getPageId().getPageNumber());
                sr = new SearchResult();
                sr.item = icursor.currentKey;
                sr.k = icursor.k;
                sr.exactMatch = true;
                if (!node.getItem(sr.k).equals(icursor.currentKey)) {
                    po.getExceptionHandler().unexpectedErrorThrow(
                            getClass(),
                            "doSearchAtLeafLevel",
                            new IndexException(new MessageInstance(m_EB0005,
                                    node.getItem(sr.k), icursor.currentKey)));
                }
            } else {
                /*
                 * Either fetchFirst or page has changed since we last fetched
                 * Search within the same page again just in case 
                 */
                tracer.event(130, node.getPage().getPageId().getContainerId(),
                        node.getPage().getPageId().getPageNumber());
                sr = node.search(icursor.currentKey);
            }

            if (sr.exactMatch
                    && icursor.scanMode == IndexCursorImpl.SCAN_FETCH_GREATER) {
                /*
                 * If we have an exact match, there are two possibilities.
                 * If this is the first call to fetch (fetchCount == 0), then
                 * we are done. Else, we need to move to the next key.
                 */
                if (sr.k == node.getKeyCount()) {
                    tracer.event(132, node.getPage().getPageId()
                            .getContainerId(), node.getPage().getPageId()
                            .getPageNumber());
                    /*
                     * Current key is the last key on this node. Therefore,
                     * move to the node to the right.
                     */
                    return moveToNextNode(icursor, node, sr);
                } else {
                    tracer.event(131, node.getPage().getPageId()
                            .getContainerId(), node.getPage().getPageId()
                            .getPageNumber());
                    /*
                     * Move to the next key in the same node
                     */
                    sr.k = sr.k + 1;
                    sr.exactMatch = false;
                    sr.item = node.getItem(sr.k);
                }
            } else if (sr.k == SearchResult.KEY_OUT_OF_BOUNDS) {
                tracer.event(133, node.getPage().getPageId().getContainerId(),
                        node.getPage().getPageId().getPageNumber());
                /*
                 * The current key is greater than all keys in this node,
                 * therefore we need to move to the node to our right.
                 */
                return moveToNextNode(icursor, node, sr);
            } else if (node.header.keyCount == 1) {
                tracer.event(134, node.getPage().getPageId().getContainerId(),
                        node.getPage().getPageId().getPageNumber());
                /*
                 * Only one key, which by definition is the high key,
                 * so we are at EOF.
                 */
                icursor.setEof(true);
                sr.k = FIRST_KEY_POS;
                sr.exactMatch = false;
                sr.item = node.getItem(sr.k);
                return sr;
            }
            return sr;

        }

        /**
         * Moves to the node to the right if possible or sets EOF status. Leaves
         * current page latched in SHARED mode.
         */
        private SearchResult moveToNextNode(IndexCursorImpl icursor,
                BTreeNode node, SearchResult sr) {
            // Key to be fetched is in the next page
            int nextPage = node.header.rightSibling;
            if (nextPage == -1) {
                tracer.event(135, node.getPage().getPageId().getContainerId(),
                        node.getPage().getPageId().getPageNumber());
                /*
                 * There isn't a node to our right, so we are at EOF.
                 */
                icursor.setEof(true);
                /*
                 * Position on the high key
                 */
                sr.k = node.header.keyCount;
                sr.exactMatch = false;
                sr.item = node.getItem(sr.k);
            } else {
                tracer.event(136, node.getPage().getPageId().getContainerId(),
                        node.getPage().getPageId().getPageNumber(), nextPage);
                PageId nextPageId = new PageId(containerId, nextPage);
                icursor.bcursor.setQ(icursor.bcursor.removeP());
                tracer.event(137, nextPageId.getContainerId(), nextPageId
                        .getPageNumber());
                icursor.bcursor.setP(btreeMgr.bufmgr.fixShared(nextPageId, 0));
                icursor.bcursor.unfixQ();
                node = new BTreeNode(po, indexItemFactory, icursor.bcursor
                        .getP().getPage());
                sr = node.search(icursor.currentKey);
                if (sr.k == SearchResult.KEY_OUT_OF_BOUNDS) {
                    if (node.header.rightSibling == -1) {
                        /*
                         * There isn't a node to our right, so we are at EOF.
                         */
                        icursor.setEof(true);
                        /*
                         * Position on the high key
                         */
                        sr.k = node.header.keyCount;
                        sr.exactMatch = false;
                        sr.item = node.getItem(sr.k);
                    } else {
                        /*
                         * Position on the first key in the next node
                         * We cannot leave the pointer on the high key as the
                         * high key is not part of the result set.
                         */
                        /*
                         * From Issue 71:
                         * The use case is when there is an indirect child which is a leaf page 
                         * and isn't the last leaf page of the tree. Also, the high key of the 
                         * leaf page is greater than all the keys in the page - maybe because 
                         * some keys have been deleted. In this situation, if a fetch tries to 
                         * look for one of the deleted keys, the correct action is to move to 
                         * the right sibling of the indirect child that would have contained 
                         * the key, and position the cursor on the first key of the right sibling. 
                         * We therefore have a use case where the search can actually move 
                         * across three leaf pages - the left direct child, the indirect child, 
                         * and finally the right sibling of the indirect child.
                         * 
                         * See the test data issue71.xml for a sample tree that demonstrates 
                         * this use case. A search for 'b4' and '24' should result in the 
                         * cursor being positioned on 'c1' and '31'.
                         */
                        nextPage = node.header.rightSibling;
                        tracer.event(140, node.getPage().getPageId()
                                .getContainerId(), node.getPage().getPageId()
                                .getPageNumber(), nextPage);
                        nextPageId = new PageId(containerId, nextPage);
                        icursor.bcursor.setQ(icursor.bcursor.removeP());
                        tracer.event(137, nextPageId.getContainerId(),
                                nextPageId.getPageNumber());
                        icursor.bcursor.setP(btreeMgr.bufmgr.fixShared(
                                nextPageId, 0));
                        icursor.bcursor.unfixQ();
                        node = new BTreeNode(po, indexItemFactory,
                                icursor.bcursor.getP().getPage());
                        sr.k = FIRST_KEY_POS;
                        sr.exactMatch = false;
                        sr.item = node.getItem(sr.k);
                    }
                }
            }
            return sr;
        }

        /**
         * Saves the cursor position
         */
        private boolean saveCursorState(IndexCursorImpl icursor, SearchResult sr) {
            icursor.currentKey = sr.item;
            icursor.pageId = icursor.bcursor.getP().getPage().getPageId();
            icursor.pageLsn = icursor.bcursor.getP().getPage().getPageLsn();
            icursor.k = sr.k;
            icursor.fetchCount++;
            icursor.scanMode = IndexCursorImpl.SCAN_FETCH_GREATER;

            return true;
        }

        /**
         * Fetches the next available key, after locking the corresponding
         * Location in the specified mode. Handles the situation where the
         * current key has been deleted.
         * 
         * @param trx Transaction that is managing this fetch
         * @param cursor The BTreeScan cursor
         * @return True if successful, fals if operation needs to be tried again
         */
        private final boolean doFetch(Transaction trx, IndexScan cursor) {
            IndexCursorImpl icursor = (IndexCursorImpl) cursor;
            try {
                boolean searchFromRoot = false;
                BTreeNode node = null;
                if (icursor.scanMode == IndexCursorImpl.SCAN_FETCH_GREATER) {
                    // This is not the first call to fetch
                    // Check to see if the BTree should be scanned to locate the key
                    icursor.bcursor.setP(btreeMgr.bufmgr.fixShared(
                            icursor.pageId, 0));
                    node = new BTreeNode(po, indexItemFactory, icursor.bcursor
                            .getP().getPage());
                    if (node.isDeallocated() || !node.isLeaf()) {
                        // The node that contained current key is no longer part of the tree, hence scan is necessary
                        searchFromRoot = true;
                        icursor.bcursor.unfixP();
                    } else {
                        // The node still exists, so we need to check whether the previously returned key is bound to the 
                        // node.
                        if (!node.getPage().getPageLsn()
                                .equals(icursor.pageLsn)
                                && !node.covers(icursor.currentKey)) {
                            // The previous key is no longer bound to the node
                            searchFromRoot = true;
                            icursor.bcursor.unfixP();
                        }
                    }
                } else {
                    // First call to fetch, hence tree must be scanned.
                    searchFromRoot = true;
                }

                if (searchFromRoot) {
                    icursor.bcursor.setSearchKey(icursor.currentKey);
                    readModeTraverse(icursor.bcursor);
                }

                SearchResult sr = doSearchAtLeafLevel(icursor);

                /*
                 * Create a savepoint. In case we cannot get the lock conditionally,
                 * and have to wait for the lock, it is possible that after the
                 * wait the search is no longer valid. We need to then rollback to the
                 * savepoint in order to release the lock we don't need any more. 
                 */
                Savepoint sp = trx.createSavepoint(false);
                try {
                    if (sr.item == null) {
                        tracer.event(139, icursor.btree.containerId);
                        po.getExceptionHandler()
                                .unexpectedErrorThrow(
                                        getClass(),
                                        "doFetch",
                                        new IndexException(new MessageInstance(
                                                m_EB0006, icursor.currentKey
                                                        .toString())));
                    }
                    trx.acquireLockNowait(sr.item.getLocation(),
                            icursor.lockMode, LockDuration.MANUAL_DURATION);

                    /*
                     * FIXME - If isolationMode is READ_COMMITTED and the current
                     * key does not match search criteria (NOT FOUND), then we 
                     * should release the lock on the key here.
                     * At present this is handled by closing the scan.
                     */
                    return saveCursorState(icursor, sr);
                } catch (LockException e) {

                    if (btreeMgr.log.isDebugEnabled()) {
                        btreeMgr.log.debug(getClass(), "doFetch",
                                "SIMPLEDBM-DEBUG: Failed to acquire conditional "
                                        + icursor.lockMode + " lock on "
                                        + sr.item.getLocation());
                    }
                    /*
                     * Failed to acquire conditional lock. 
                     * Need to unlatch page and retry unconditionally. 
                     */
                    icursor.bcursor.unfixP();
                    trx.acquireLock(sr.item.getLocation(), icursor.lockMode,
                            LockDuration.MANUAL_DURATION);
                    /*
                     * We have obtained the lock. We need to double check that the searched key
                     * still exists.
                     */
                    // TODO - could avoid the tree traverse here by checking the old page
                    icursor.bcursor.setSearchKey(icursor.currentKey);
                    readModeTraverse(icursor.bcursor);
                    SearchResult sr1 = doSearchAtLeafLevel(icursor);
                    // FIXME - can sr1.item be null here?
                    if (sr1.item.equals(sr.item)) {
                        // we found the same key again
                        /*
                         * FIXME - If isolationMode is READ_COMMITTED and the current
                         * key does not match search criteria (NOT FOUND), then we 
                         * should release the lock on the key here.
                         * At present this is handled by closing the scan.
                         */
                        return saveCursorState(icursor, sr1);
                    }
                    /*
                     * Need to restart search as the last location is no longer
                     * what we want.
                     */
                    trx.rollback(sp);
                }
            } finally {
                icursor.bcursor.unfixAll();
            }
            /*
             * Search has to retried
             */
            return false;
        }

        /**
         * Fetches the next key.
         */
        public final void fetch(Transaction trx, IndexScan cursor) {
            boolean success = doFetch(trx, cursor);
            while (!success) {
                /*
                 * Fetch has to be retried
                 */
                success = doFetch(trx, cursor);
            }
        }

        public final IndexScan openScan(Transaction trx, IndexKey key,
                Location location, boolean forUpdate) {
            if (key == null) {
                // Use minimum key
                key = keyFactory.minIndexKey(containerId);
            }
            if (location == null) {
                // Use null location
                location = locationFactory.newLocation();
            }
            IndexCursorImpl icursor = new IndexCursorImpl(trx, this,
                    new IndexItem(key, location, -1, true, isUnique()),
                    forUpdate ? LockMode.UPDATE : LockMode.SHARED);
            return icursor;
        }
    }

    public static final class IndexCursorImpl implements IndexScan,
            TransactionalCursor {

        /**
         * The page that we were at last time fetch was called.
         */
        PageId pageId;

        /**
         * The LSN of the page we were at last. Saved so that we can detect
         * whether the page has since changed.
         */
        Lsn pageLsn;

        /**
         * The start key for the scan. Only recorded for information.
         */
        IndexItem startKey;

        /**
         * Used by fetch routines. Initially set to the user supplied search
         * key. As the scan moves, tracks the current key the cursor is
         * positioned on. The associated location must always be locked.
         */
        IndexItem currentKey;

        /**
         * Saved reference to previous key - used to release lock on previous
         * key in certain Isolation modes.
         */
        IndexItem previousKey;

        /**
         * Cached value of {@link SearchResult#k}. If the last page we were at
         * hasn't changed, then we can use the cached value to avoid searching
         * the page.
         */
        int k = 0;

        /**
         * Keeps a count of the number of keys fetched so far.
         */
        int fetchCount = 0;

        /**
         * Internal use
         */
        final BTreeContext bcursor;

        /**
         * The lock mode to be used for locking locations retrieved by the scan.
         */
        LockMode lockMode;

        /**
         * The BTree that we are scanning.
         */
        final BTreeImpl btree;

        /**
         * Indicates whether we have reached end of file. Also set when
         * {@link #fetchCompleted(boolean)} is invoked with an argument of
         * false.
         */
        private boolean eof = false;

        /**
         * Transaction that is managing this scan.
         */
        final Transaction trx;

        /**
         * Initially set to {@link #SCAN_FETCH_GREATER_OR_EQUAL}, and after the
         * first fetch, set to {@link #SCAN_FETCH_GREATER}. The scan mode
         * determines how the fetch will operate.
         */
        int scanMode = 0;

        int stateFetchCompleted = 0;

        static final int SCAN_FETCH_GREATER_OR_EQUAL = 1;

        static final int SCAN_FETCH_GREATER = 2;

        IndexCursorImpl(Transaction trx, BTreeImpl btree, IndexItem startKey,
                LockMode lockMode) {
            this.btree = btree;
            this.bcursor = new BTreeContext(btree.tracer);
            this.trx = trx;
            this.startKey = startKey;
            this.currentKey = startKey; // initial search key
            this.previousKey = null;
            this.fetchCount = 0;
            this.lockMode = lockMode;
            this.scanMode = SCAN_FETCH_GREATER_OR_EQUAL;
            trx.registerTransactionalCursor(this);
        }

        /* (non-Javadoc)
         * @see org.simpledbm.rss.api.im.IndexScan#fetchNext()
         */
        public final boolean fetchNext() {
            if (stateFetchCompleted != 0) {
                //            	btree.btreeMgr.log.warn(
                //                    getClass().getName(),
                //                    "close",
                //                    "fetchCompleted() has not been called after fetchNext()");
                fetchCompleted();
            }
            if (!isEof()) {
                if (scanMode == SCAN_FETCH_GREATER) {
                    /*
                     * scanMode == SCAN_FETCH_GREATER_OR_EQUAL is set initially before the
                     * first fetch. It is also set when the cursor has been restored and a
                     * rescan is needed to set the current key. In both these cases, previousKey is
                     * not meaningful.
                     */
                    previousKey = currentKey;
                }
                if (previousKey != null) {
                    if (trx.getIsolationMode() == IsolationMode.CURSOR_STABILITY) {
                        LockMode lockMode = trx.hasLock(previousKey
                                .getLocation());
                        if (lockMode == LockMode.SHARED
                                || lockMode == LockMode.UPDATE) {
                            if (btree.btreeMgr.log.isDebugEnabled()) {
                                btree.btreeMgr.log.debug(getClass(),
                                        IndexCursorImpl.class.getName()
                                                + ".fetchNext",
                                        "SIMPLEDBM-DEBUG: Releasing lock on previous row "
                                                + previousKey.getLocation());
                            }
                            trx.releaseLock(previousKey.getLocation());
                        }
                    } else if ((trx.getIsolationMode() == IsolationMode.REPEATABLE_READ || trx
                            .getIsolationMode() == IsolationMode.SERIALIZABLE)
                            && lockMode == LockMode.UPDATE) {
                        /*
                         * This is an update mode cursor.
                         * In RR/SR mode, we need to downgrade UPDATE mode lock to SHARED lock when the cursor moves
                         * to the next row.
                         */
                        LockMode lockMode = trx.hasLock(previousKey
                                .getLocation());
                        if (lockMode == LockMode.UPDATE) {
                            if (btree.btreeMgr.log.isDebugEnabled()) {
                                btree.btreeMgr.log.debug(getClass(),
                                        IndexCursorImpl.class.getName()
                                                + ".fetchNext",
                                        "SIMPLEDBM-DEBUG: Downgrading lock on previous row "
                                                + previousKey.getLocation()
                                                + " to " + LockMode.SHARED);
                            }
                            trx.downgradeLock(previousKey.getLocation(),
                                    LockMode.SHARED);
                        }
                    }
                }
                btree.fetch(trx, this);
                if (!isEof()) {
                    stateFetchCompleted = 1;
                }
                return !isEof();
            }
            return false;
        }

        private void fetchCompleted() {
            /*
             * This method is invoked after the data from tuple container has been
             * read. Its main purpose is to release locks in read committed or cursor stability mode.
             */
            stateFetchCompleted = 0;
            //            if (!matched && !isEof()) {
            //                setEof(true);
            //            }
            if (currentKey != null) {
                boolean releaseLock = false;
                if (trx.getIsolationMode() == IsolationMode.CURSOR_STABILITY
                        || trx.getIsolationMode() == IsolationMode.REPEATABLE_READ) {
                    if (isEof()) {
                        releaseLock = true;
                    }
                } else if (trx.getIsolationMode() == IsolationMode.READ_COMMITTED) {
                    releaseLock = true;
                }
                if (releaseLock) {
                    LockMode lockMode = trx.hasLock(currentKey.getLocation());
                    if (lockMode == LockMode.SHARED
                            || lockMode == LockMode.UPDATE) {
                        trx.releaseLock(currentKey.getLocation());
                    }
                } else if (isEof()) {
                    LockMode lockMode = trx.hasLock(currentKey.getLocation());
                    if (lockMode == LockMode.UPDATE) {
                        trx.downgradeLock(currentKey.getLocation(),
                                LockMode.SHARED);
                    }
                }
            }
        }

        public final void fetchCompleted(boolean matched) {
        }

        public final void close() {
            trx.unregisterTransactionalCursor(this);
            SimpleDBMException ex = null;

            // Following is needed because in the not found scenario,
            // fetchCompleted will not be called.
            try {
                if (currentKey != null) {
                    if (trx.getIsolationMode() == IsolationMode.READ_COMMITTED
                            || trx.getIsolationMode() == IsolationMode.CURSOR_STABILITY
                            || (isEof() && trx.getIsolationMode() == IsolationMode.REPEATABLE_READ)) {
                        LockMode lockMode = trx.hasLock(currentKey
                                .getLocation());
                        if (lockMode == LockMode.SHARED
                                || lockMode == LockMode.UPDATE) {
                            if (btree.btreeMgr.log.isDebugEnabled()) {
                                btree.btreeMgr.log
                                        .debug(
                                                getClass(),
                                                this.getClass().getName()
                                                        + ".close",
                                                "SIMPLEDBM-DEBUG: Releasing lock on current row "
                                                        + currentKey
                                                                .getLocation()
                                                        + " because isolation mode = CS or RC or (RR and EOF) and mode = SHARED or UPDATE");
                            }
                            trx.releaseLock(currentKey.getLocation());
                        }
                    }
                }
            } catch (SimpleDBMException e) {
                ex = e;
            }

            try {
                bcursor.unfixAll();
            } catch (SimpleDBMException e) {
                if (ex == null) {
                    ex = e;
                }
            }
            if (ex != null) {
                throw ex;
            }
            if (stateFetchCompleted != 0) {
                // btree.btreeMgr.log.warn(getClass().getName(), "close", btree.po.getMessageCatalog().getMessage("WB0011"));
            }
        }

        public final IndexKey getCurrentKey() {
            if (currentKey == null) {
                return null;
            }
            return currentKey.getKey();
        }

        public final Location getCurrentLocation() {
            if (currentKey == null) {
                return null;
            }
            return currentKey.getLocation();
        }

        public void restoreState(Transaction txn, Savepoint sp) {
            CursorState cs = (CursorState) sp.getValue(this);

            if (btree.btreeMgr.log.isDebugEnabled()) {
                btree.btreeMgr.log.debug(getClass(), this.getClass()
                        .getName()
                        + ".restoreState",
                        "SIMPLEDBM-DEBUG: Current position is set to "
                                + currentKey);
                btree.btreeMgr.log.debug(getClass(), this.getClass()
                        .getName()
                        + ".restoreState",
                        "SIMPLEDBM-DEBUG: Rollback to savepoint is restoring state to "
                                + cs);
            }
            currentKey = cs.currentKey;
            previousKey = cs.previousKey;
            startKey = cs.searchKey;
            pageId = cs.pageId;
            pageLsn = cs.pageLsn;
            setEof(cs.eof);
            /*
             * If scan mode is SCAN_FETCH_GREATER it means that
             * the crsor had fetched rows prior to being saved. So we
             * need to set the cursor position to where it was originally.
             */
            if (cs.scanMode == SCAN_FETCH_GREATER) {
                fetchCount = cs.fetchCount - 1;
                scanMode = SCAN_FETCH_GREATER_OR_EQUAL;
                fetchNext();
            } else {
                /*
                 * Cursor was not used - so reset back to start state. 
                 */
                assert cs.fetchCount == 0;
                assert cs.scanMode == SCAN_FETCH_GREATER_OR_EQUAL;
                fetchCount = 0;
                scanMode = SCAN_FETCH_GREATER_OR_EQUAL;
            }
        }

        public void saveState(Savepoint sp) {
            CursorState cs = new CursorState(this);
            sp.saveValue(this, cs);
        }

        void setEof(boolean eof) {
            this.eof = eof;
        }

        public final boolean isEof() {
            return eof;
        }

        /**
         * CursorState object holds the saved cursor state.
         * 
         * @author dibyendumajumdar
         */
        static final class CursorState {
            final IndexCursorImpl scan;
            final IndexItem currentKey;
            final IndexItem previousKey;
            final IndexItem searchKey;
            final PageId pageId;
            final Lsn pageLsn;
            final boolean eof;
            final int fetchCount;
            final int scanMode;

            CursorState(IndexCursorImpl scan) {
                this.scan = scan;
                if (scan.currentKey != null) {
                    this.currentKey = new IndexItem(scan.currentKey);
                } else {
                    this.currentKey = null;
                }
                if (scan.previousKey != null) {
                    this.previousKey = new IndexItem(scan.previousKey);
                } else {
                    this.previousKey = null;
                }
                if (scan.startKey != null) {
                    this.searchKey = new IndexItem(scan.startKey);
                } else {
                    this.searchKey = null;
                }
                if (scan.pageId != null) {
                    this.pageId = new PageId(scan.pageId);
                } else {
                    this.pageId = null;
                }
                if (scan.pageLsn != null) {
                    this.pageLsn = new Lsn(scan.pageLsn);
                } else {
                    this.pageLsn = null;
                }
                this.eof = scan.isEof();
                this.fetchCount = scan.fetchCount;
                this.scanMode = scan.scanMode;
            }

            public String toString() {
                return "CursorState(savedCurrentKey = " + currentKey + ")";
            }
        }
    }

    public static final class BTreeContext {

        private BufferAccessBlock q = null;

        private BufferAccessBlock r = null;

        private BufferAccessBlock p = null;

        /**
         * Search key - this is used internally by the various BTree routines.
         */
        public IndexItem searchKey = null;

        /**
         * Used to save the next key location during inserts so that the lock
         * can be released subsequent to the insert.
         */
        private Location nextKeyLocation = null;

        private final TraceBuffer tracer;

        public BTreeContext(TraceBuffer tracer) {
            this.tracer = tracer;
        }

        public final BufferAccessBlock getP() {
            return p;
        }

        public final BufferAccessBlock removeP() {
            if (p != null) {
                tracer.event(114, p.getPage().getPageId().getContainerId(), p
                        .getPage().getPageId().getPageNumber());
            }
            BufferAccessBlock bab = p;
            p = null;
            return bab;
        }

        public final void setP(BufferAccessBlock p) {
            if (p != null) {
                tracer.event(115, p.getPage().getPageId().getContainerId(), p
                        .getPage().getPageId().getPageNumber());
            }
            assert this.p == null;
            this.p = p;
        }

        public final BufferAccessBlock getQ() {
            return q;
        }

        public final BufferAccessBlock removeQ() {
            if (q != null) {
                tracer.event(116, q.getPage().getPageId().getContainerId(), q
                        .getPage().getPageId().getPageNumber());
            }
            BufferAccessBlock bab = q;
            q = null;
            return bab;
        }

        public final void setQ(BufferAccessBlock q) {
            if (q != null) {
                tracer.event(117, q.getPage().getPageId().getContainerId(), q
                        .getPage().getPageId().getPageNumber());
            }
            assert this.q == null;
            this.q = q;
        }

        public final BufferAccessBlock getR() {
            return r;
        }

        public final BufferAccessBlock removeR() {
            if (r != null) {
                tracer.event(118, r.getPage().getPageId().getContainerId(), r
                        .getPage().getPageId().getPageNumber());
            }
            BufferAccessBlock bab = r;
            r = null;
            return bab;
        }

        public final void setR(BufferAccessBlock r) {
            if (r != null) {
                tracer.event(119, r.getPage().getPageId().getContainerId(), r
                        .getPage().getPageId().getPageNumber());
            }
            assert this.r == null;
            this.r = r;
        }

        public final IndexItem getSearchKey() {
            return searchKey;
        }

        public final void setSearchKey(IndexItem searchKey) {
            this.searchKey = searchKey;
        }

        public final void unfixP() {
            if (p != null) {
                tracer.event(120, p.getPage().getPageId().getContainerId(), p
                        .getPage().getPageId().getPageNumber());
                p.unfix();
                p = null;
            }
        }

        public final void unfixQ() {
            if (q != null) {
                tracer.event(121, q.getPage().getPageId().getContainerId(), q
                        .getPage().getPageId().getPageNumber());
                q.unfix();
                q = null;
            }
        }

        public final void unfixR() {
            if (r != null) {
                tracer.event(122, r.getPage().getPageId().getContainerId(), r
                        .getPage().getPageId().getPageNumber());
                r.unfix();
                r = null;
            }
        }

        public final void unfixAll() {
            SimpleDBMException e = null;
            try {
                unfixP();
            } catch (SimpleDBMException e1) {
                e = e1;
            }
            try {
                unfixQ();
            } catch (SimpleDBMException e1) {
                if (e == null)
                    e = e1;
            }
            try {
                unfixR();
            } catch (SimpleDBMException e1) {
                if (e == null)
                    e = e1;
            }
            if (e != null) {
                throw e;
            }
        }

        public Location getNextKeyLocation() {
            return nextKeyLocation;
        }

        public void setNextKeyLocation(Location nextKeyLocation) {
            this.nextKeyLocation = nextKeyLocation;
        }
    }

    public static final class SearchResult implements Dumpable {

        static final int KEY_OUT_OF_BOUNDS = -1;

        /**
         * The key number can range from
         * {@link BTreeIndexManagerImpl#FIRST_KEY_POS} to
         * {@link BTreeNode#getKeyCount()}. A value of -1 indicates that the
         * search key is greater than all keys in the node.
         */
        int k = KEY_OUT_OF_BOUNDS;
        IndexItem item = null;
        boolean exactMatch = false;

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("SearchResult(k = ").append(k).append(", item = [");
            if (item != null) {
                item.appendTo(sb);
            }
            sb.append("], exactMatch = ").append(exactMatch).append(")");
            return sb;
        }

        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }
    }

    /**
     * A BTreeNode presents a btree node view of a slotted page. It is used to
     * manage the contents of a B-link tree node. The BTreeNode understands and
     * handles the differences between leaf nodes and index nodes.
     * 
     * <pre>
     * Leaf nodes have following structure:
     * [header] [item1] [item2] ... [itemN] [highkey]
     * header.keyCount ------------------------^
     * item[0] = header       
     * item[1..header.KeyCount-1] = keys
     * item[header.keyCount] = high key
     * </pre>
     * 
     * The highkey in a leaf node is an extra item, and may or may not be the
     * same as the last key [itemN] in the page. Operations that change the
     * highkey in leaf pages are Split, Merge and Redistribute. All keys in the
     * page are guaranteed to be &lt;= highkey.
     * <p>
     * Index nodes have following structure:
     * 
     * <pre>
     * [header] [item1] [item2] ... [itemN]
     * header.keyCount ----------------^
     * item[0] = header 
     * item[1..header.KeyCount] = keys
     * </pre>
     * 
     * In an index node, the last key is also the highkey. Each item in an index
     * node contains a pointer to a child page. The child page contains keys
     * that are &lt;= than the item key. If the high key of a child page is less
     * than the item key in the parent, then it can be deduced that the page has
     * a right sibling that is not linked to the parent.
     * <p>
     * Note that the rightmost index page at any level has a special key as the
     * highkey - this key has a value of INFINITY.
     * 
     * @author Dibyendu Majumdar
     * @since 19-Sep-2005
     */
    public static final class BTreeNode implements StorableFactory, Dumpable {

        static final short NODE_TYPE_LEAF = 1;
        static final short NODE_TREE_UNIQUE = 2;
        static final short NODE_TREE_DEALLOCATED = 4;

        final PlatformObjects po;

        /**
         * Page being managed.
         */
        final SlottedPage page;

        /**
         * Noted pageId of page we are handling. Useful for validation.
         */
        final PageId pageId;

        /**
         * Noted pageLsn of page we are handling. Useful for validation.
         */
        final Lsn pageLsn;

        /**
         * Cached header.
         */
        BTreeNodeHeader header;

        final IndexItemFactory indexItemFactory;

        final PartialIndexItem.PartialIndexItemFactory partialIndexItemFactory;

        final TraceBuffer tracer;

        BTreeNode(PlatformObjects po, IndexItemFactory indexItemFactory,
                Page page) {
            this.po = po;
            this.tracer = po.getTraceBuffer();
            this.indexItemFactory = indexItemFactory;
            this.page = (SlottedPage) page;
            this.pageId = page.getPageId();
            this.pageLsn = page.getPageLsn();
            this.header = (BTreeNodeHeader) this.page.get(HEADER_KEY_POS,
                    new BTreeNodeHeader.BTreeNodeHeaderStorabeFactory());
            this.partialIndexItemFactory = new PartialIndexItem.PartialIndexItemFactory(
                    isLeaf());
        }

        private final boolean sanityCheck() {
            if (page != null && pageId.equals(page.getPageId())
                    && pageLsn.equals(page.getPageLsn())) {
                return true;
            }
            po.getExceptionHandler().unexpectedErrorThrow(
                    this.getClass(), "sanityCheck",
                    new IndexException(new MessageInstance(m_EB0014)));
            return false;
        }

        /**
         * Formats a new BTree page.
         */
        static void formatPage(SlottedPage page, int keyFactoryType,
                int locationFactoryType, boolean leaf, boolean isUnique,
                int spaceMapPageNumber) {
            page.init();
            short flags = 0;
            if (leaf) {
                flags |= BTreeNode.NODE_TYPE_LEAF;
            }
            if (isUnique) {
                flags |= BTreeNode.NODE_TREE_UNIQUE;
            }
            page.setFlags(flags);
            page.setSpaceMapPageNumber(spaceMapPageNumber);
            BTreeNodeHeader header = new BTreeNodeHeader();
            header.setKeyFactoryType(keyFactoryType);
            header.setLocationFactoryType(locationFactoryType);
            page.insertAt(HEADER_KEY_POS, header, true);
        }

        public final void dumpAsXml() {
            System.out.println("<page containerId=\""
                    + page.getPageId().getContainerId() + "\" pageNumber=\""
                    + page.getPageId().getPageNumber() + "\">");
            System.out.println("	<header>");
            System.out.println("		<locationfactory>"
                    + header.locationFactoryType + "</locationfactory>");
            System.out.println("		<keyfactory>" + header.keyFactoryType
                    + "</keyfactory>");
            System.out.println("		<unique>" + (isUnique() ? "yes" : "false")
                    + "</unique>");
            System.out.println("		<leaf>" + (isLeaf() ? "yes" : "false")
                    + "</leaf>");
            System.out.println("		<leftsibling>" + header.leftSibling
                    + "</leftsibling>");
            System.out.println("		<rightsibling>" + header.rightSibling
                    + "</rightsibling>");
            System.out.println("		<smppagenumber>"
                    + page.getSpaceMapPageNumber() + "</smppagenumber>");
            System.out
                    .println("		<keycount>" + header.keyCount + "</keycount>");
            System.out.println("	</header>");
            System.out.println("	<items>");
            for (int k = FIRST_KEY_POS; k < page.getNumberOfSlots(); k++) {
                if (page.isSlotDeleted(k)) {
                    continue;
                }
                IndexItem item = getItem(k);
                System.out.println("		<item pos=\"" + k + "\">");
                System.out.println("			<childpagenumber>"
                        + item.childPageNumber + "</childpagenumber>");
                System.out.println("			<location>" + item.getLocation()
                        + "</location>");
                System.out.println("			<key>" + item.getKey().toString()
                        + "</key>");
                System.out.println("		</item>");
            }
            System.out.println("	</items>");
            System.out.println("</page>");
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            return appendTo(sb).toString();
        }

        public final StringBuilder appendTo(StringBuilder sb) {
            sb.append("<page containerId=\"").append(
                    page.getPageId().getContainerId()).append(
                    "\" pageNumber=\"")
                    .append(page.getPageId().getPageNumber()).append("\">")
                    .append(Dumpable.newline);
            sb.append("	<header>").append(Dumpable.newline);
            sb.append("		<locationfactory>").append(header.locationFactoryType)
                    .append("</locationfactory>").append(Dumpable.newline);
            sb.append("		<keyfactory>").append(header.keyFactoryType).append(
                    "</keyfactory>").append(Dumpable.newline);
            sb.append("		<unique>").append(isUnique() ? "yes" : "false")
                    .append("</unique>").append(Dumpable.newline);
            sb.append("		<leaf>").append(isLeaf() ? "yes" : "false").append(
                    "</leaf>").append(Dumpable.newline);
            sb.append("		<leftsibling>").append(header.leftSibling).append(
                    "</leftsibling>").append(Dumpable.newline);
            sb.append("		<rightsibling>").append(header.rightSibling).append(
                    "</rightsibling>").append(Dumpable.newline);
            sb.append("		<smppagenumber>").append(page.getSpaceMapPageNumber())
                    .append("</smppagenumber>").append(Dumpable.newline);
            sb.append("		<keycount>").append(header.keyCount).append(
                    "</keycount>").append(Dumpable.newline);
            sb.append("	</header>").append(Dumpable.newline);
            sb.append("	<items>").append(Dumpable.newline);
            for (int k = FIRST_KEY_POS; k < page.getNumberOfSlots(); k++) {
                if (page.isSlotDeleted(k)) {
                    continue;
                }
                IndexItem item = getItem(k);
                sb.append("		<item pos=\"").append(k).append("\">").append(
                        Dumpable.newline);
                sb.append("			<childpagenumber>").append(item.childPageNumber)
                        .append("</childpagenumber>").append(Dumpable.newline);
                sb.append("			<location>").append(item.getLocation()).append(
                        "</location>").append(Dumpable.newline);
                sb.append("			<key>").append(item.getKey().toString()).append(
                        "</key>").append(Dumpable.newline);
                sb.append("		</item>").append(Dumpable.newline);
            }
            sb.append("	</items>").append(Dumpable.newline);
            sb.append("</page>").append(Dumpable.newline);
            return sb;
        }

        /**
         * Dumps contents of the BTree node.
         */
        public final void dump() {

            // dumpAsXml();
            if (DiagnosticLogger.getDiagnosticsLevel() == 0) {
                return;
            }
            page.dump();
            assert page.getSpaceMapPageNumber() != -1;
            if (page.getNumberOfSlots() > 0) {
                BTreeNodeHeader header = (BTreeNodeHeader) page.get(
                        HEADER_KEY_POS,
                        new BTreeNodeHeader.BTreeNodeHeaderStorabeFactory());
                DiagnosticLogger.log("BTreeNodeHeader=" + header);
                for (int k = FIRST_KEY_POS; k < page.getNumberOfSlots(); k++) {
                    if (page.isSlotDeleted(k)) {
                        continue;
                    }
                    IndexItem item = (IndexItem) page.get(k, this);
                    if (k == header.keyCount) {
                        DiagnosticLogger.log("IndexItem[" + k
                                + "] (HIGHKEY) = " + item);
                    } else {
                        DiagnosticLogger.log("IndexItem[" + k + "] = " + item);
                    }
                }
            }
        }

        public final void validateItemAt(int slot) {
            try {
                Thread.yield();
                IndexItem thisItem = getItem(slot);
                if (slot > 1) {
                    IndexItem prevItem = getItem(slot - 1);
                    if (prevItem.compareTo(thisItem) >= 0) {
                        po.getExceptionHandler().unexpectedErrorThrow(
                                this.getClass(),
                                "validateItemAt",
                                new IndexException(new MessageInstance(
                                        m_EB0015, slot)));
                    }
                }
                if (isLeaf()) {
                    if (slot == header.keyCount - 1) {
                        // key before high key can be equal or less than high key in a leaf page
                        IndexItem nextItem = getItem(slot + 1);
                        if (thisItem.compareTo(nextItem) > 0) {
                            po.getExceptionHandler().unexpectedErrorThrow(
                                    this.getClass(),
                                    "validateItemAt",
                                    new IndexException(new MessageInstance(
                                            m_EB0015, slot)));
                        }
                    } else if (slot < header.keyCount - 1) {
                        IndexItem nextItem = getItem(slot + 1);
                        if (thisItem.compareTo(nextItem) >= 0) {
                            po.getExceptionHandler().unexpectedErrorThrow(
                                    this.getClass(),
                                    "validateItemAt",
                                    new IndexException(new MessageInstance(
                                            m_EB0015, slot)));
                        }
                    }
                } else {
                    if (slot < header.keyCount) {
                        IndexItem nextItem = getItem(slot + 1);
                        if (thisItem.compareTo(nextItem) >= 0) {
                            po.getExceptionHandler().unexpectedErrorThrow(
                                    this.getClass(),
                                    "validateItemAt",
                                    new IndexException(new MessageInstance(
                                            m_EB0015, slot)));
                        }
                    }
                }
            } catch (SimpleDBMException e) {
                dumpAsXml();
                throw e;
            }
        }

        public final void validateHeader() {
            try {
                if (page.getNumberOfSlots() == 0) {
                    return;
                }
                if (isDeallocated()) {
                    po.getExceptionHandler().unexpectedErrorThrow(
                            this.getClass(),
                            "validateItemAt",
                            new IndexException(new MessageInstance(m_EB0016,
                                    page.getPageId())));
                }
                if (page.getSpaceMapPageNumber() == -1) {
                    po.getExceptionHandler().unexpectedErrorThrow(
                            this.getClass(),
                            "validateItemAt",
                            new IndexException(new MessageInstance(m_EB0017,
                                    page.getPageId())));
                }
                if (page.getNumberOfSlots() != header.keyCount + 1) {
                    po.getExceptionHandler().unexpectedErrorThrow(
                            this.getClass(),
                            "validateItemAt",
                            new IndexException(new MessageInstance(m_EB0018,
                                    page.getPageId())));
                }
            } catch (SimpleDBMException e) {
                dumpAsXml();
                throw e;
            }
        }

        /**
         * Validates the contents of the BTree node.
         */
        public final void validate() {

            if (page.getNumberOfSlots() == 0) {
                return;
            }
            Thread.yield();
            try {
                if (isDeallocated()) {
                    po.getExceptionHandler().unexpectedErrorThrow(
                            this.getClass(),
                            "validate",
                            new IndexException(new MessageInstance(m_EB0016,
                                    page.getPageId())));
                }
                if (page.getSpaceMapPageNumber() == -1) {
                    po.getExceptionHandler().unexpectedErrorThrow(
                            this.getClass(),
                            "validate",
                            new IndexException(new MessageInstance(m_EB0017,
                                    page.getPageId())));
                }
                if (page.getNumberOfSlots() != header.keyCount + 1) {
                    po.getExceptionHandler().unexpectedErrorThrow(
                            this.getClass(),
                            "validate",
                            new IndexException(new MessageInstance(m_EB0018,
                                    page.getPageId())));
                }
                BTreeNodeHeader header = (BTreeNodeHeader) page.get(
                        HEADER_KEY_POS,
                        new BTreeNodeHeader.BTreeNodeHeaderStorabeFactory());
                int keyCount = 0;
                int deletedCount = 0;
                IndexItem prevItem = null;
                for (int k = FIRST_KEY_POS; k < page.getNumberOfSlots(); k++) {
                    if (page.isSlotDeleted(k)) {
                        deletedCount++;
                        continue;
                    }
                    Thread.yield();
                    keyCount++;
                    IndexItem item = (IndexItem) page.get(k, this);
                    if (prevItem != null) {
                        if (k == header.keyCount) {
                            if (isLeaf()) {
                                if (item.compareTo(prevItem) < 0) {
                                    po
                                            .getExceptionHandler()
                                            .unexpectedErrorThrow(
                                                    this.getClass(),
                                                    "validate",
                                                    new IndexException(
                                                            new MessageInstance(
                                                                    m_EB0015, k)));
                                }
                            } else {
                                if (item.compareTo(prevItem) <= 0) {
                                    po
                                            .getExceptionHandler()
                                            .unexpectedErrorThrow(
                                                    this.getClass(),
                                                    "validate",
                                                    new IndexException(
                                                            new MessageInstance(
                                                                    m_EB0015, k)));
                                }
                            }
                        } else {
                            if (item.compareTo(prevItem) <= 0) {
                                po.getExceptionHandler().unexpectedErrorThrow(
                                        this.getClass(),
                                        "validate",
                                        new IndexException(new MessageInstance(
                                                m_EB0015, k)));
                            }
                        }
                    }
                    prevItem = item;
                }
                if (deletedCount > 0) {
                    po.getExceptionHandler().unexpectedErrorThrow(
                            this.getClass(),
                            "validate",
                            new IndexException(new MessageInstance(m_EB0015,
                                    page.getPageId())));
                }
                if (keyCount != header.keyCount) {
                    po.getExceptionHandler().unexpectedErrorThrow(
                            this.getClass(),
                            "validate",
                            new IndexException(new MessageInstance(m_EB0018,
                                    page.getPageId())));
                }
            } catch (SimpleDBMException e) {
                dumpAsXml();
                throw e;
            }
        }

        final BTreeNodeHeader getHeader() {
            return header;
        }

        /**
         * Returns the high key. High key is always the last physical key on the
         * page.
         */
        final IndexItem getHighKey() {
            return getItem(header.keyCount);
        }

        /**
         * Returns the largest key on the page.
         */
        public final IndexItem getLastKey() {
            return getItem(getKeyCount());
        }

        /**
         * Returns specified item.
         */
        public final IndexItem getItem(int slotNumber) {
            sanityCheck();
            return (IndexItem) page.get(slotNumber, this);
        }

        final PartialIndexItem getPartialItem(int slotNumber) {
            sanityCheck();
            return (PartialIndexItem) page.get(slotNumber,
                    partialIndexItemFactory);
        }

        private void validateItem(IndexItem item) {
            if (item.isLeaf != isLeaf() || item.isUnique != isUnique()) {
                po.getExceptionHandler().unexpectedErrorThrow(
                        this.getClass(), "validateItem",
                        new IndexException(new MessageInstance(m_EB0020)));
            }
        }

        /**
         * Inserts item at specified position. Existing items are shifted to the
         * right if necessary.
         */
        public final void insert(int slotNumber, IndexItem item) {
            sanityCheck();
            validateItem(item);
            page.insertAt(slotNumber, item, false);
        }

        public final void purge(int slotNumber) {
            sanityCheck();
            page.purge(slotNumber);
        }

        /**
         * Replaces the item at specified position.
         */
        public final void replace(int slotNumber, IndexItem item) {
            sanityCheck();
            validateItem(item);
            page.insertAt(slotNumber, item, true);
        }

        /**
         * Tests whether the page is part of a unique index.
         */
        public final boolean isUnique() {
            sanityCheck();
            int flags = page.getFlags();
            return (flags & NODE_TREE_UNIQUE) != 0;
        }

        /**
         * Sets the unique flag.
         */
        public final void setUnique() {
            sanityCheck();
            int flags = page.getFlags();
            page.setFlags((short) (flags | NODE_TREE_UNIQUE));
        }

        /**
         * Resets the unique flag.
         */
        public final void unsetUnique() {
            sanityCheck();
            int flags = page.getFlags();
            page.setFlags((short) (flags & ~NODE_TREE_UNIQUE));
        }

        /**
         * Tests whether this is a leaf page.
         */
        public final boolean isLeaf() {
            sanityCheck();
            int flags = page.getFlags();
            return (flags & NODE_TYPE_LEAF) != 0;
        }

        /**
         * Sets the leaf flag.
         */
        public final void setLeaf() {
            sanityCheck();
            int flags = page.getFlags();
            page.setFlags((short) (flags | NODE_TYPE_LEAF));
        }

        /**
         * Clears the leaf flag.
         */
        public final void unsetLeaf() {
            sanityCheck();
            int flags = page.getFlags();
            page.setFlags((short) (flags & ~NODE_TYPE_LEAF));
        }

        /**
         * Is this the root page?
         */
        public final boolean isRoot() {
            sanityCheck();
            return page.getPageId().getPageNumber() == BTreeIndexManagerImpl.ROOT_PAGE_NUMBER;
        }

        /**
         * Tests whether this page has been marked as deallocated.
         */
        public final boolean isDeallocated() {
            sanityCheck();
            int flags = page.getFlags();
            return (flags & NODE_TREE_DEALLOCATED) != 0;
        }

        /**
         * Sets the deallocated flag.
         */
        public final void setDeallocated() {
            sanityCheck();
            int flags = page.getFlags();
            page.setFlags((short) (flags | NODE_TREE_DEALLOCATED));
        }

        /**
         * Clears the deallocated flag.
         */
        public final void unsetDeallocated() {
            sanityCheck();
            int flags = page.getFlags();
            page.setFlags((short) (flags & ~NODE_TREE_DEALLOCATED));
        }

        /**
         * Returns number of keys stored in the page. For leaf pages, the high
         * key is excluded.
         */
        final int getKeyCount() {
            if (isLeaf()) {
                return header.keyCount - 1;
            }
            return header.keyCount;
        }

        /**
         * Returns the number of physical keys in the page, including the extra
         * high key in leaf pages.
         */
        public final int getNumberOfKeys() {
            sanityCheck();
            return header.keyCount;
        }

        final Page getPage() {
            sanityCheck();
            return page;
        }

        /**
         * Finds the key that should be used as the median key when splitting a
         * page.
         */
        final short getSplitKey() {
            sanityCheck();
            if (BTreeIndexManagerImpl.testingFlag == TEST_MODE_LIMIT_MAX_KEYS_PER_PAGE) {
                /*
                 * We are in test mode and therefore artificially limit the
                 * number of keys to a small value.
                 */
                return TEST_MODE_SPLIT_KEY;
            }
            int halfSpace = page.getSpace() / 2;
            int space = 0;
            for (short k = FIRST_KEY_POS; k <= header.keyCount; k++) {
                space += page.getSlotLength(k);
                if (space > halfSpace) {
                    return k;
                }
            }
            po.getExceptionHandler().unexpectedErrorThrow(getClass(),
                    "getSplitKey",
                    new IndexException(new MessageInstance(m_EB0007, page)));
            return -1;
        }

        /**
         * Sets the keycount in the header record.
         */
        public final void setNumberOfKeys(int keyCount) {
            sanityCheck();
            header.keyCount = keyCount;
        }

        /**
         * Updates the header stored within the page.
         */
        public final void updateHeader() {
            sanityCheck();
            page.insertAt(HEADER_KEY_POS, header, true);
        }

        /**
         * Searches for the supplied key. If there is an exact match,
         * SearchResult.exactMatch will be set. Otherwise, if the node contains
         * a key &gt;= 0 the supplied key, SearchResult.k and SearchResult.item
         * will be set to it. If all keys in the page are &lt; search key then,
         * SearchResult.k will be set to -1 and SearchResult.item will be null.
         * <p>
         * Note that in leaf nodes, the search does not consider the high key.
         * Hence the -1 result needs to be verified by the caller by comparing
         * against the high key.
         */
        public final SearchResult search(IndexItem key) {
            sanityCheck();
            tracer.event(138, pageId.getContainerId(), pageId.getPageNumber());
            SearchResult result = new SearchResult();
            /*
             * Binary search algorithm
             */
            int low = FIRST_KEY_POS;
            /*
             * Note that in leaf nodes, we do not include the
             * high key in searches.
             */
            int high = getKeyCount();
            while (low <= high) {
                int mid = (low + high) >>> 1;
                if (mid < FIRST_KEY_POS || mid > getKeyCount()) {
                    po.getExceptionHandler().unexpectedErrorThrow(
                            getClass(),
                            "search",
                            new IndexException(new MessageInstance(m_EB0008,
                                    key.toString())));
                }
                IndexItem item = getItem(mid);
                int comp = item.compareTo(key);
                if (comp < 0)
                    low = mid + 1;
                else if (comp > 0)
                    high = mid - 1;
                else {
                    result.k = mid;
                    result.exactMatch = true;
                    result.item = item;
                    return result;
                }
            }
            /*
             * If the result is not equal, we need to return the
             * next key if available or -1 if the search key is greater than
             * all keys in this node.
             */
            if (low <= getKeyCount()) {
                result.k = low;
                result.item = getItem(low);
            }
            return result;
        }

        /**
         * Finds the child page associated with an index item.
         */
        public final int findChildPage(IndexItem key) {
            if (isLeaf()) {
                po.getExceptionHandler()
                        .unexpectedErrorThrow(
                                getClass(),
                                "findChildPage",
                                new IndexException(new MessageInstance(
                                        m_EB0009, page)));
            }
            SearchResult sr = search(key);
            assert sr.k != SearchResult.KEY_OUT_OF_BOUNDS;
            return sr.item.getChildPageNumber();
        }

        /**
         * Finds the index item associated with a child page.
         */
        public final IndexItem findIndexItem(int childPageNumber) {
            if (isLeaf()) {
                po.getExceptionHandler()
                        .unexpectedErrorThrow(
                                getClass(),
                                "findIndexItem",
                                new IndexException(new MessageInstance(
                                        m_EB0009, page)));
            }
            /*
             * We avoid reading the full item until we have found the one we want.
             */
            for (int k = FIRST_KEY_POS; k <= getKeyCount(); k++) {
                PartialIndexItem item = getPartialItem(k);
                if (item.childPageNumber == childPageNumber) {
                    return getItem(k);
                }
            }
            return null;
        }

        /**
         * Finds the position of a child page pointer.
         */
        public final int findPosition(int childPageNumber) {
            if (isLeaf()) {
                po.getExceptionHandler()
                        .unexpectedErrorThrow(
                                getClass(),
                                "findPosition",
                                new IndexException(new MessageInstance(
                                        m_EB0009, page)));
            }
            for (int k = FIRST_KEY_POS; k <= getKeyCount(); k++) {
                PartialIndexItem item = getPartialItem(k);
                if (item.childPageNumber == childPageNumber) {
                    return k;
                }
            }
            return -1;
        }

        /**
         * Finds the index item for left sibling of the specified child page.
         */
        public final IndexItem findPrevIndexItem(int childPageNumber) {
            if (isLeaf()) {
                po.getExceptionHandler()
                        .unexpectedErrorThrow(
                                getClass(),
                                "findPrevIndexItem",
                                new IndexException(new MessageInstance(
                                        m_EB0009, page)));
            }
            int k = FIRST_KEY_POS;
            for (; k <= getKeyCount(); k++) {
                PartialIndexItem item = getPartialItem(k);
                if (item.childPageNumber == childPageNumber) {
                    break;
                }
            }
            if (k == FIRST_KEY_POS) {
                return null;
            }
            return getItem(k - 1);
        }

        /**
         * Tests if the current page can be merged with its right sibling.
         */
        public final boolean canMergeWith(BTreeNode rightSibling) {
            sanityCheck();
            if (BTreeIndexManagerImpl.testingFlag == TEST_MODE_LIMIT_MAX_KEYS_PER_PAGE) {
                /*
                 * We are in test mode and therefore artificially limit the
                 * number of keys to a small value.
                 */
                int n = getNumberOfKeys() - (isLeaf() ? 1 : 0)
                        + rightSibling.getNumberOfKeys();
                return n <= TEST_MODE_MAX_KEYS;
            }
            int requiredSpace = 0;
            if (isLeaf()) {
                /*
                 * leaf nodes have an extra high key.
                 * the high key in the left node will be deleted during the
                 * merge - therefore we need to subtract the space required
                 * for this key.
                 */
                requiredSpace -= page.getSlotLength(header.keyCount);
            }
            for (int k = FIRST_KEY_POS; k <= rightSibling.getNumberOfKeys(); k++) {
                // add all keys from the right sibling
                requiredSpace += rightSibling.page.getSlotLength(k);
            }
            // TODO should we leave some slack here?
            return requiredSpace < page.getFreeSpace();
        }

        public final boolean canAccomodate(IndexItem v) {
            sanityCheck();
            /*
             * Note: The published algorithm says that a node is about to
             * overflow if it cannot accomodate a maximum length key.
             * We do not do this - instead we check whether we can actually
             * accomodate the key.
             */
            if (BTreeIndexManagerImpl.testingFlag == TEST_MODE_LIMIT_MAX_KEYS_PER_PAGE) {
                /*
                 * We are in test mode and therefore artificially limit the
                 * number of keys to a small value.
                 */
                return getNumberOfKeys() < TEST_MODE_MAX_KEYS;
            }
            int requiredSpace = v.getStoredLength() + page.getSlotOverhead();
            if (v.isLeaf() && !isLeaf()) {
                // Allow for extra child pointer
                requiredSpace += TypeSize.INTEGER;
            }
            // TODO should we leave some slack here?
            return requiredSpace <= page.getFreeSpace();
        }

        /**
         * Determines if the specified key "bound" to this page.
         */
        public final boolean covers(IndexItem v) {
            IndexItem first = getItem(FIRST_KEY_POS);
            IndexItem last = getItem(getKeyCount());
            return first.compareTo(v) <= 0 && last.compareTo(v) >= 0;
        }

        public final int minimumKeys() {
            return (2 - (isRoot() ? 1 : 0));
        }

        /**
         * Tests whether this page is about to underflow. This will be true if
         * it is a root page with only one child or any other page with only two
         * children/keys.
         * <p>
         */
        public final boolean isAboutToUnderflow() {
            if (isRoot()) {
                if (!isLeaf()) {
                    return getKeyCount() == minimumKeys();
                } else {
                    return false;
                }
            }
            return getKeyCount() == minimumKeys();
        }

        public Storable getStorable(ByteBuffer buf) {
            return new IndexItem(isLeaf(), isUnique(), pageId.getContainerId(),
                    indexItemFactory.keyFactory,
                    indexItemFactory.locationFactory, buf);
        }

    }

    /**
     * Callback for checking whether a page qualifies for reuse.
     * 
     * @author Dibyendu Majumdar
     */
    public static final class SpaceCheckerImpl implements FreeSpaceChecker {

        public final boolean hasSpace(int value) {
            return value == PAGE_SPACE_UNUSED;
        }

    }

    /**
     * Every page in the BTree has a header item at slot 0.
     */
    public static final class BTreeNodeHeader implements Storable, Dumpable {

        static final int SIZE = TypeSize.INTEGER * 5;

        /**
         * Pointer to left sibling page. Note that although we have this, this
         * is not kept fully up-to-date because to do so would require extra
         * work when merging pages. FIXME
         */
        int leftSibling = -1;

        /**
         * Pointer to right sibling page.
         */
        int rightSibling = -1;

        /**
         * Total number of keys present in the page. Includes the high key in
         * leaf pages.
         */
        int keyCount = 0;

        /**
         * Type code for the key factory to be used to manipulate index keys.
         */
        int keyFactoryType = -1;

        /**
         * Typecode of the location factory to be used for generating Location
         * objects.
         */
        int locationFactoryType = -1;

        /* (non-Javadoc)
         * @see org.simpledbm.io.Storable#getStoredLength()
         */
        public final int getStoredLength() {
            return SIZE;
        }

        BTreeNodeHeader() {
            super();
        }

        BTreeNodeHeader(ByteBuffer bb) {
            keyFactoryType = bb.getInt();
            locationFactoryType = bb.getInt();
            leftSibling = bb.getInt();
            rightSibling = bb.getInt();
            keyCount = bb.getInt();
        }

        /* (non-Javadoc)
         * @see org.simpledbm.io.Storable#store(java.nio.ByteBuffer)
         */
        public final void store(ByteBuffer bb) {
            bb.putInt(keyFactoryType);
            bb.putInt(locationFactoryType);
            bb.putInt(leftSibling);
            bb.putInt(rightSibling);
            bb.putInt(keyCount);
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("BTreeNodeHeader(keyFactory=").append(keyFactoryType);
            sb.append(", locationFactory=").append(locationFactoryType);
            sb.append(", leftSibling=").append(leftSibling);
            sb.append(", rightSibling=").append(rightSibling);
            sb.append(", keyCount=").append(keyCount);
            sb.append(")");
            return sb;
        }

        @Override
        public final String toString() {
            return appendTo(new StringBuilder()).toString();
        }

        final int getKeyCount() {
            return keyCount;
        }

        final void setKeyCount(int keyCount) {
            this.keyCount = keyCount;
        }

        final int getLeftSibling() {
            return leftSibling;
        }

        final void setLeftSibling(int leftSibling) {
            this.leftSibling = leftSibling;
        }

        final int getRightSibling() {
            return rightSibling;
        }

        final void setRightSibling(int rightSibling) {
            this.rightSibling = rightSibling;
        }

        final int getKeyFactoryType() {
            return keyFactoryType;
        }

        final void setKeyFactoryType(int keyFactoryType) {
            this.keyFactoryType = keyFactoryType;
        }

        final int getLocationFactoryType() {
            return locationFactoryType;
        }

        final void setLocationFactoryType(int locationFactoryType) {
            this.locationFactoryType = locationFactoryType;
        }

        static final class BTreeNodeHeaderStorabeFactory implements
                StorableFactory {

            public Storable getStorable(ByteBuffer buf) {
                return new BTreeNodeHeader(buf);
            }
        }
    }

    final static class IndexItemFactory {

        private final IndexKeyFactory keyFactory;

        private final LocationFactory locationFactory;

        private final boolean unique;

        IndexItemFactory(IndexKeyFactory keyFactory,
                LocationFactory locationFactory, boolean unique) {
            this.keyFactory = keyFactory;
            this.locationFactory = locationFactory;
            this.unique = unique;
        }

        public final Location getNewLocation() {
            return locationFactory.newLocation();
        }

        public final IndexKey getMaxIndexKey(int containerId) {
            return keyFactory.maxIndexKey(containerId);
        }

        public final IndexKey getNewIndexKey(int containerId, String s) {
            return keyFactory.parseIndexKey(containerId, s);
        }

        public final IndexItem newIndexItem(int containerId, boolean leaf,
                ByteBuffer bb) {
            return new IndexItem(leaf, unique, containerId, keyFactory,
                    locationFactory, bb);
        }

        boolean isUnique() {
            return unique;
        }

        public final IndexItem getInfiniteKey(int containerId, boolean leaf) {
            return new IndexItem(getMaxIndexKey(containerId), getNewLocation(),
                    -1, leaf, isUnique());
        }
    }

    /**
     * Base class for all log operations for BTree. A key feature is that all
     * log operations have access to information that allows them to generate
     * keys, locations and index items. Log operations also _know_ whether they
     * are to be applied to leaf pages, or to unique indexes.
     * <p>
     * Having all the relevant information to hand is useful specially during
     * restart recovery and debugging. The downside is that this information is
     * repeated in all log records.
     * <p>
     * It is possible to avoid storing the index data in log records if we can
     * ensure that the information can be obtained some other way. For example,
     * the Index manager could cache information about indexes, perhaps keyed by
     * container id, and log records could obtain this information by querying
     * the index manager. However, there is the issue that during restart
     * recovery, the index may not exist, and therefore at least one log record
     * (the one that creates the index) needs to contain information about the
     * index.
     * 
     * @author Dibyendu Majumdar
     */
    public static abstract class BTreeLogOperation extends BaseLoggable
    /* implements IndexItemHelper */{

        /**
         * Is this a leaf level operation.
         */
        private final boolean leaf;

        /**
         * Is this part of a unique index?
         */
        private final boolean unique;

        /**
         * Type code for the key factory to be used to manipulate index keys.
         */
        private final int keyFactoryType;

        /**
         * Typecode of the location factory to be used for generating Location
         * objects.
         */
        private final int locationFactoryType;

        private final ObjectRegistry objectFactory;

        private final IndexKeyFactory keyFactory;

        private final LocationFactory locationFactory;

        private final IndexItemFactory indexItemFactory;

        protected BTreeLogOperation(int moduleId, int typeCode,
                ObjectRegistry objectRegistry, int keyFactoryType,
                int locationFactoryType, boolean leaf, boolean unique) {
            super(moduleId, typeCode);
            this.objectFactory = objectRegistry;
            this.keyFactoryType = keyFactoryType;
            this.locationFactoryType = locationFactoryType;
            keyFactory = (IndexKeyFactory) objectFactory
                    .getSingleton(keyFactoryType);
            locationFactory = (LocationFactory) objectFactory
                    .getSingleton(locationFactoryType);
            this.leaf = leaf;
            this.unique = unique;
            this.indexItemFactory = new IndexItemFactory(keyFactory,
                    locationFactory, unique);
        }

        protected BTreeLogOperation(ObjectRegistry objectRegistry, ByteBuffer bb) {
            super(bb);
            this.objectFactory = objectRegistry;
            keyFactoryType = bb.getInt();
            locationFactoryType = bb.getInt();
            keyFactory = (IndexKeyFactory) objectFactory
                    .getSingleton(keyFactoryType);
            locationFactory = (LocationFactory) objectFactory
                    .getSingleton(locationFactoryType);
            leaf = bb.get() == 1;
            unique = bb.get() == 1;
            this.indexItemFactory = new IndexItemFactory(keyFactory,
                    locationFactory, unique);
        }

        protected BTreeLogOperation(int moduleId, int typeCode,
                BTreeLogOperation other) {
            super(moduleId, typeCode);
            this.keyFactory = other.keyFactory;
            this.keyFactoryType = other.keyFactoryType;
            this.leaf = other.leaf;
            this.locationFactory = other.locationFactory;
            this.locationFactoryType = other.locationFactoryType;
            this.objectFactory = other.objectFactory;
            this.unique = other.unique;
            this.indexItemFactory = other.indexItemFactory;
        }

        @Override
        public int getStoredLength() {
            int n = super.getStoredLength();
            n += 2;
            n += TypeSize.INTEGER * 2;
            return n;
        }

        @Override
        public void store(ByteBuffer bb) {
            super.store(bb);
            bb.putInt(keyFactoryType);
            bb.putInt(locationFactoryType);
            bb.put(leaf ? (byte) 1 : (byte) 0);
            bb.put(unique ? (byte) 1 : (byte) 0);
        }

        public final Location getNewLocation() {
            return locationFactory.newLocation();
        }

        public final IndexKey getMaxIndexKey() {
            return keyFactory.maxIndexKey(getPageId().getContainerId());
        }

        public final IndexItem makeNewItem(ByteBuffer bb) {
            return new IndexItem(leaf, unique, getPageId().getContainerId(),
                    keyFactory, locationFactory, bb);
        }

        public final IndexItem makeNewItem(boolean leaf, boolean unique,
                ByteBuffer bb) {
            return new IndexItem(leaf, unique, getPageId().getContainerId(),
                    keyFactory, locationFactory, bb);
        }

        public final int getKeyFactoryType() {
            return keyFactoryType;
        }

        public final int getLocationFactoryType() {
            return locationFactoryType;
        }

        public final boolean isUnique() {
            return unique;
        }

        public final boolean isLeaf() {
            return leaf;
        }

        public final IndexKeyFactory getIndexKeyFactory() {
            return keyFactory;
        }

        public final LocationFactory getLocationFactory() {
            return locationFactory;
        }

        protected final ObjectRegistry getObjectFactory() {
            return objectFactory;
        }

        public IndexItemFactory getIndexItemFactory() {
            return indexItemFactory;
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("isLeaf=").append(isLeaf() ? "true" : "false");
            sb.append(", isUnique=").append(isUnique() ? "true" : "false");
            sb.append(", keyFactoryType=").append(getKeyFactoryType());
            sb.append(", locationFactoryType=")
                    .append(getLocationFactoryType());
            sb.append(", ");
            super.appendTo(sb);
            return sb;
        }

        @Override
        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }

    }

    /**
     * Base class for operations that require a positional update of a key.
     * 
     * @author Dibyendu Majumdar
     */
    public static abstract class KeyUpdateOperation extends BTreeLogOperation {

        /**
         * Item to be updated
         */
        private IndexItem item;

        /**
         * Position of the item within the BTree node
         */
        private int position = -1;

        protected KeyUpdateOperation(ObjectRegistry objectRegistry,
                ByteBuffer bb) {
            super(objectRegistry, bb);
            position = bb.getInt();
            item = makeNewItem(true, isUnique(), bb);
        }

        protected KeyUpdateOperation(int moduleId, int typeCode,
                ObjectRegistry objectRegistry, int keyFactoryType,
                int locationFactoryType, boolean leaf, boolean unique) {
            super(moduleId, typeCode, objectRegistry, keyFactoryType,
                    locationFactoryType, leaf, unique);
        }

        protected KeyUpdateOperation(int moduleId, int typeCode,
                KeyUpdateOperation o) {
            super(moduleId, typeCode, o);
            this.item = o.item;
            this.position = o.position;
        }

        public final int getPosition() {
            return position;
        }

        public final void setPosition(int position) {
            this.position = position;
        }

        public final IndexItem getItem() {
            return item;
        }

        public final void setItem(IndexItem item) {
            this.item = item;
            if (!item.isLeaf()) {
                // FIXME need to use exceptionHandler
                MessageInstance m = new MessageInstance(itemNotLeaf, item);
                throw new IndexException(m);
            }
        }

        @Override
        public final void store(ByteBuffer bb) {
            super.store(bb);
            bb.putInt(position);
            item.store(bb);
        }

        @Override
        public final int getStoredLength() {
            return super.getStoredLength() + TypeSize.INTEGER
                    + item.getStoredLength();
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("position=").append(position);
            sb.append(", item=[").append(item.toString());
            sb.append("], ");
            super.appendTo(sb);
            return sb;
        }

        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }
    }

    /**
     * Log record data for inserting a key into the BTree.
     */
    public static final class InsertOperation extends KeyUpdateOperation
            implements LogicalUndo {

        public InsertOperation(ObjectRegistry objectRegistry, ByteBuffer bb) {
            super(objectRegistry, bb);
        }

        public InsertOperation(int moduleId, int typeCode,
                ObjectRegistry objectRegistry, int keyFactoryType,
                int locationFactoryType, boolean leaf, boolean unique) {
            super(moduleId, typeCode, objectRegistry, keyFactoryType,
                    locationFactoryType, leaf, unique);
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("InsertOperation(");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }

        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }

        static final class InsertOperationFactory implements ObjectFactory {
            private final ObjectRegistry objectRegistry;

            InsertOperationFactory(ObjectRegistry objectRegistry) {
                this.objectRegistry = objectRegistry;
            }

            public Class<?> getType() {
                return InsertOperation.class;
            }

            public Object newInstance(ByteBuffer buf) {
                return new InsertOperation(objectRegistry, buf);
            }
        }
    }

    public static final class UndoInsertOperation extends KeyUpdateOperation
            implements Compensation {

        public UndoInsertOperation(ObjectRegistry objectRegistry, ByteBuffer bb) {
            super(objectRegistry, bb);
        }

        public UndoInsertOperation(int moduleId, int typeCode,
                ObjectRegistry objectRegistry, int keyFactoryType,
                int locationFactoryType, boolean leaf, boolean unique) {
            super(moduleId, typeCode, objectRegistry, keyFactoryType,
                    locationFactoryType, leaf, unique);
        }

        public UndoInsertOperation(int moduleId, int typeCode,
                InsertOperation other) {
            super(moduleId, typeCode, other);
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("UndoInsertOperation(");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }

        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }

        static final class UndoInsertOperationFactory implements ObjectFactory {
            private final ObjectRegistry objectRegistry;

            UndoInsertOperationFactory(ObjectRegistry objectRegistry) {
                this.objectRegistry = objectRegistry;
            }

            public Class<?> getType() {
                return UndoInsertOperation.class;
            }

            public Object newInstance(ByteBuffer buf) {
                return new UndoInsertOperation(objectRegistry, buf);
            }
        }

    }

    public static final class DeleteOperation extends KeyUpdateOperation
            implements LogicalUndo {

        public DeleteOperation(ObjectRegistry objectRegistry, ByteBuffer bb) {
            super(objectRegistry, bb);
        }

        public DeleteOperation(int moduleId, int typeCode,
                ObjectRegistry objectRegistry, int keyFactoryType,
                int locationFactoryType, boolean leaf, boolean unique) {
            super(moduleId, typeCode, objectRegistry, keyFactoryType,
                    locationFactoryType, leaf, unique);
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("DeleteOperation(");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }

        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }

        static final class DeleteOperationFactory implements ObjectFactory {
            private final ObjectRegistry objectRegistry;

            DeleteOperationFactory(ObjectRegistry objectRegistry) {
                this.objectRegistry = objectRegistry;
            }

            public Class<?> getType() {
                return DeleteOperation.class;
            }

            public Object newInstance(ByteBuffer buf) {
                return new DeleteOperation(objectRegistry, buf);
            }
        }

    }

    public static final class UndoDeleteOperation extends KeyUpdateOperation
            implements Compensation {

        public UndoDeleteOperation(ObjectRegistry objectRegistry, ByteBuffer bb) {
            super(objectRegistry, bb);
        }

        public UndoDeleteOperation(int moduleId, int typeCode,
                ObjectRegistry objectRegistry, int keyFactoryType,
                int locationFactoryType, boolean leaf, boolean unique) {
            super(moduleId, typeCode, objectRegistry, keyFactoryType,
                    locationFactoryType, leaf, unique);
        }

        public UndoDeleteOperation(int moduleId, int typeCode,
                DeleteOperation other) {
            super(moduleId, typeCode, other);
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("UndoDeleteOperation(");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }

        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }

        static final class UndoDeleteOperationFactory implements ObjectFactory {
            private final ObjectRegistry objectRegistry;

            UndoDeleteOperationFactory(ObjectRegistry objectRegistry) {
                this.objectRegistry = objectRegistry;
            }

            public Class<?> getType() {
                return UndoDeleteOperation.class;
            }

            public Object newInstance(ByteBuffer buf) {
                return new UndoDeleteOperation(objectRegistry, buf);
            }
        }

    }

    /**
     * Split operation log record.
     */
    public static final class SplitOperation extends BTreeLogOperation
            implements Compensation, MultiPageRedo {

        /**
         * Page Id of new sibling page.
         */
        int newSiblingPageNumber;

        /**
         * The items that will become part of the new sibling page. Includes the
         * highkey in leaf pages.
         */
        LinkedList<IndexItem> items;

        /**
         * The new sibling page will point to current page's right sibling.
         */
        int rightSibling;

        /**
         * Space map page that owns the new page.
         */
        int spaceMapPageNumber;

        /**
         * The value of the high key - used only in leaf pages.
         */
        IndexItem highKey;

        /**
         * After splitting, this is the new keycount of the page. Includes
         * highkey if this is a leaf page.
         */
        short newKeyCount;

        public SplitOperation(ObjectRegistry objectRegistry, ByteBuffer bb) {
            super(objectRegistry, bb);
            short numberOfItems = bb.getShort();
            items = new LinkedList<IndexItem>();
            for (int i = 0; i < numberOfItems; i++) {
                IndexItem item = makeNewItem(bb);
                items.add(item);
            }
            if (isLeaf()) {
                highKey = makeNewItem(bb);
            }
            newSiblingPageNumber = bb.getInt();
            rightSibling = bb.getInt();
            spaceMapPageNumber = bb.getInt();
            newKeyCount = bb.getShort();
        }

        public SplitOperation(int moduleId, int typeCode,
                ObjectRegistry objectRegistry, int keyFactoryType,
                int locationFactoryType, boolean leaf, boolean unique) {
            super(moduleId, typeCode, objectRegistry, keyFactoryType,
                    locationFactoryType, leaf, unique);
            items = new LinkedList<IndexItem>();
        }

        @Override
        public final int getStoredLength() {
            int n = super.getStoredLength();
            n += TypeSize.SHORT;
            for (IndexItem item : items) {
                n += item.getStoredLength();
            }
            if (isLeaf()) {
                n += highKey.getStoredLength();
            }
            n += TypeSize.INTEGER * 3;
            n += TypeSize.SHORT;
            return n;
        }

        @Override
        public final void store(ByteBuffer bb) {
            super.store(bb);
            bb.putShort((short) items.size());
            for (IndexItem item : items) {
                item.store(bb);
            }
            if (isLeaf()) {
                highKey.store(bb);
            }
            bb.putInt(newSiblingPageNumber);
            bb.putInt(rightSibling);
            bb.putInt(spaceMapPageNumber);
            bb.putShort(newKeyCount);
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("SplitOperation(");
            sb.append("Number of items=").append(items.size()).append(newline);
            for (IndexItem item : items) {
                sb.append("  item #=[");
                item.appendTo(sb);
                sb.append("]").append(newline);
            }
            if (isLeaf()) {
                sb.append("  highKey=");
                highKey.appendTo(sb);
                sb.append(newline);
            }
            sb.append(", newSiblingPageNumber=").append(newSiblingPageNumber);
            sb.append(", rightSibling=").append(rightSibling);
            sb.append(", spaceMapPageNumber=").append(spaceMapPageNumber);
            sb.append(", newKeyCount=").append(newKeyCount);
            sb.append(", ");
            super.appendTo(sb);
            sb.append(", PageId[]={");
            for (PageId pageId : getPageIds()) {
                pageId.appendTo(sb);
                sb.append(",");
            }
            sb.append("})");
            return sb;
        }

        @Override
        public final String toString() {
            return appendTo(new StringBuilder()).toString();
        }

        /**
         * Returns pages that are affected by this log. Included are the page
         * that is being split, and the newly allocated page.
         */
        public final PageId[] getPageIds() {
            return new PageId[] {
                    getPageId(),
                    new PageId(getPageId().getContainerId(),
                            newSiblingPageNumber) };
        }

        static final class SplitOperationFactory implements ObjectFactory {
            private final ObjectRegistry objectRegistry;

            SplitOperationFactory(ObjectRegistry objectRegistry) {
                this.objectRegistry = objectRegistry;
            }

            public Class<?> getType() {
                return SplitOperation.class;
            }

            public Object newInstance(ByteBuffer buf) {
                return new SplitOperation(objectRegistry, buf);
            }
        }
    }

    public static final class MergeOperation extends BTreeLogOperation
            implements Redoable, MultiPageRedo {

        /**
         * The items that will become part of the new sibling page. Includes the
         * highkey in leaf pages.
         */
        LinkedList<IndexItem> items;

        /**
         * The new sibling page will point to current page's right sibling.
         */
        int rightSibling;

        int rightSiblingSpaceMapPage;

        int rightRightSibling;

        public MergeOperation(ObjectRegistry objectRegistry, ByteBuffer bb) {
            super(objectRegistry, bb);
            short numberOfItems = bb.getShort();
            items = new LinkedList<IndexItem>();
            for (int i = 0; i < numberOfItems; i++) {
                IndexItem item = makeNewItem(bb);
                items.add(item);
            }
            rightSibling = bb.getInt();
            rightSiblingSpaceMapPage = bb.getInt();
            rightRightSibling = bb.getInt();
        }

        public MergeOperation(int moduleId, int typeCode,
                ObjectRegistry objectRegistry, int keyFactoryType,
                int locationFactoryType, boolean leaf, boolean unique) {
            super(moduleId, typeCode, objectRegistry, keyFactoryType,
                    locationFactoryType, leaf, unique);
            items = new LinkedList<IndexItem>();
        }

        @Override
        public final int getStoredLength() {
            int n = super.getStoredLength();
            n += TypeSize.SHORT;
            for (IndexItem item : items) {
                n += item.getStoredLength();
            }
            n += TypeSize.INTEGER * 3;
            return n;
        }

        @Override
        public final void store(ByteBuffer bb) {
            super.store(bb);
            bb.putShort((short) items.size());
            for (IndexItem item : items) {
                item.store(bb);
            }
            bb.putInt(rightSibling);
            bb.putInt(rightSiblingSpaceMapPage);
            bb.putInt(rightRightSibling);
        }

        public final PageId[] getPageIds() {
            return new PageId[] {
                    getPageId(),
                    new PageId(getPageId().getContainerId(), rightSibling),
                    new PageId(getPageId().getContainerId(),
                            rightSiblingSpaceMapPage) };
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("MergeOperation(Number of items=").append(items.size())
                    .append(newline);
            for (IndexItem item : items) {
                sb.append("  item #=[");
                item.appendTo(sb);
                sb.append("]").append(newline);
            }
            sb.append(", rightSibling=").append(rightSibling);
            sb.append(", rightSiblingSpaceMapPage=").append(
                    rightSiblingSpaceMapPage);
            sb.append(", rightRightSibling=").append(rightRightSibling);
            sb.append(", ");
            super.appendTo(sb);
            sb.append(", PageId[]={");
            for (PageId pageId : getPageIds()) {
                pageId.appendTo(sb);
                sb.append(",");
            }
            sb.append("})");
            return sb;
        }

        @Override
        public final String toString() {
            return appendTo(new StringBuilder()).toString();
        }

        static final class MergeOperationFactory implements ObjectFactory {
            private final ObjectRegistry objectRegistry;

            MergeOperationFactory(ObjectRegistry objectRegistry) {
                this.objectRegistry = objectRegistry;
            }

            public Class<?> getType() {
                return MergeOperation.class;
            }

            public Object newInstance(ByteBuffer buf) {
                return new MergeOperation(objectRegistry, buf);
            }
        }

    }

    public static final class LinkOperation extends BTreeLogOperation implements
            Redoable {

        int leftSibling;

        int rightSibling;

        IndexItem leftChildHighKey;

        public LinkOperation(ObjectRegistry objectRegistry, ByteBuffer bb) {
            super(objectRegistry, bb);
            leftChildHighKey = makeNewItem(bb);
            rightSibling = bb.getInt();
            leftSibling = bb.getInt();
        }

        public LinkOperation(int moduleId, int typeCode,
                ObjectRegistry objectRegistry, int keyFactoryType,
                int locationFactoryType, boolean leaf, boolean unique) {
            super(moduleId, typeCode, objectRegistry, keyFactoryType,
                    locationFactoryType, leaf, unique);
        }

        @Override
        public final int getStoredLength() {
            int n = super.getStoredLength();
            n += leftChildHighKey.getStoredLength();
            n += TypeSize.INTEGER * 2;
            return n;
        }

        @Override
        public final void store(ByteBuffer bb) {
            super.store(bb);
            leftChildHighKey.store(bb);
            bb.putInt(rightSibling);
            bb.putInt(leftSibling);
        }

        @Override
        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("LinkOperation(leftChildHighKey=");
            leftChildHighKey.appendTo(sb);
            sb.append(", rightSibling=").append(rightSibling);
            sb.append(", leftSibling=").append(leftSibling);
            sb.append(")");
            return sb;
        }

        @Override
        public final String toString() {
            return appendTo(new StringBuilder()).toString();
        }

        static final class LinkOperationFactory implements ObjectFactory {
            private final ObjectRegistry objectRegistry;

            LinkOperationFactory(ObjectRegistry objectRegistry) {
                this.objectRegistry = objectRegistry;
            }

            public Class<?> getType() {
                return LinkOperation.class;
            }

            public Object newInstance(ByteBuffer buf) {
                return new LinkOperation(objectRegistry, buf);
            }
        }

    }

    /**
     * Log record for the Unlink operation. It is applied to the parent page.
     * 
     * @see BTreeImpl#doUnlink(Transaction, BTreeContext)
     * @see BTreeIndexManagerImpl#redoUnlinkOperation(Page, UnlinkOperation)
     */
    public static final class UnlinkOperation extends BTreeLogOperation
            implements Redoable {

        /**
         * Pointer to right child.
         */
        int leftSibling;

        /**
         * Pointer to left child.
         */
        int rightSibling;

        public UnlinkOperation(ObjectRegistry objectRegistry, ByteBuffer bb) {
            super(objectRegistry, bb);
            rightSibling = bb.getInt();
            leftSibling = bb.getInt();
        }

        public UnlinkOperation(int moduleId, int typeCode,
                ObjectRegistry objectRegistry, int keyFactoryType,
                int locationFactoryType, boolean leaf, boolean unique) {
            super(moduleId, typeCode, objectRegistry, keyFactoryType,
                    locationFactoryType, leaf, unique);
        }

        @Override
        public final int getStoredLength() {
            int n = super.getStoredLength();
            n += TypeSize.INTEGER * 2;
            return n;
        }

        @Override
        public final void store(ByteBuffer bb) {
            super.store(bb);
            bb.putInt(rightSibling);
            bb.putInt(leftSibling);
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("UnlinkOperation(leftSibling=");
            sb.append(leftSibling);
            sb.append(", rightSibling=");
            sb.append(rightSibling);
            sb.append(", ");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }

        @Override
        public final String toString() {
            return appendTo(new StringBuilder()).toString();
        }

        static final class UnlinkOperationFactory implements ObjectFactory {
            private final ObjectRegistry objectRegistry;

            UnlinkOperationFactory(ObjectRegistry objectRegistry) {
                this.objectRegistry = objectRegistry;
            }

            public Class<?> getType() {
                return UnlinkOperation.class;
            }

            public Object newInstance(ByteBuffer buf) {
                return new UnlinkOperation(objectRegistry, buf);
            }
        }

    }

    /**
     * Unlike the published algorithm we simply transfer one key from the more
     * populated page to the less populated page.
     * 
     * @author Dibyendu Majumdar
     * @since 06-Oct-2005
     * @deprecated
     */
    public static final class RedistributeOperation extends BTreeLogOperation
            implements Redoable, MultiPageRedo {

        /**
         * Pointer to the left sibling.
         */
        int leftSibling;

        /**
         * Pointer to the right sibling.
         */
        int rightSibling;

        /**
         * The key that will be moved.
         */
        IndexItem key;

        /**
         * Pointer to the recipient of the key.
         */
        int targetSibling;

        public RedistributeOperation(ObjectRegistry objectRegistry,
                ByteBuffer bb) {
            super(objectRegistry, bb);
            rightSibling = bb.getInt();
            leftSibling = bb.getInt();
            targetSibling = bb.getInt();
            key = makeNewItem(bb);
        }

        public RedistributeOperation(int moduleId, int typeCode,
                ObjectRegistry objectRegistry, int keyFactoryType,
                int locationFactoryType, boolean leaf, boolean unique) {
            super(moduleId, typeCode, objectRegistry, keyFactoryType,
                    locationFactoryType, leaf, unique);
        }

        @Override
        public final int getStoredLength() {
            int n = super.getStoredLength();
            n += TypeSize.INTEGER * 3;
            n += key.getStoredLength();
            return n;
        }

        @Override
        public final void store(ByteBuffer bb) {
            super.store(bb);
            bb.putInt(rightSibling);
            bb.putInt(leftSibling);
            bb.putInt(targetSibling);
            key.store(bb);
        }

        public final PageId[] getPageIds() {
            return new PageId[] { getPageId(),
                    new PageId(getPageId().getContainerId(), rightSibling) };
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("RedistributeOperation(leftSibling=").append(leftSibling);
            sb.append(", rightSibling=").append(rightSibling);
            sb.append(", targetSibling=").append(targetSibling);
            sb.append(", key=[");
            key.appendTo(sb);
            sb.append("]");
            sb.append(", PageId[]={");
            for (PageId pageId : getPageIds()) {
                pageId.appendTo(sb);
                sb.append(",");
            }
            sb.append("})");
            return sb;
        }

        @Override
        public final String toString() {
            return appendTo(new StringBuilder()).toString();
        }

        /**
         * @author dibyendumajumdar
         * @deprecated
         */
        static final class RedistributeOperationFactory implements
                ObjectFactory {
            private final ObjectRegistry objectRegistry;

            RedistributeOperationFactory(ObjectRegistry objectRegistry) {
                this.objectRegistry = objectRegistry;
            }

            public Class<?> getType() {
                return RedistributeOperation.class;
            }

            public Object newInstance(ByteBuffer buf) {
                return new RedistributeOperation(objectRegistry, buf);
            }
        }
    }

    /**
     * Unlike the published algorithm we simply transfer one key from the more
     * populated page to the less populated page.
     * 
     * @author Dibyendu Majumdar
     * @since 06-Oct-2005
     */
    public static final class NewRedistributeOperation extends
            BTreeLogOperation implements Redoable, MultiPageRedo {

        /**
         * Pointer to the left sibling.
         */
        int leftSibling;

        /**
         * Pointer to the right sibling.
         */
        int rightSibling;

        /**
         * The items that will be moved. Does not include highkey in leaf pages.
         */
        LinkedList<IndexItem> items;

        /**
         * Pointer to the recipient of the key.
         */
        int targetSibling;

        public NewRedistributeOperation(ObjectRegistry objectRegistry,
                ByteBuffer bb) {
            super(objectRegistry, bb);
            rightSibling = bb.getInt();
            leftSibling = bb.getInt();
            targetSibling = bb.getInt();
            short numberOfItems = bb.getShort();
            items = new LinkedList<IndexItem>();
            for (int i = 0; i < numberOfItems; i++) {
                IndexItem item = makeNewItem(bb);
                items.add(item);
            }
        }

        public NewRedistributeOperation(int moduleId, int typeCode,
                ObjectRegistry objectRegistry, int keyFactoryType,
                int locationFactoryType, boolean leaf, boolean unique) {
            super(moduleId, typeCode, objectRegistry, keyFactoryType,
                    locationFactoryType, leaf, unique);
            items = new LinkedList<IndexItem>();
        }

        @Override
        public final int getStoredLength() {
            int n = super.getStoredLength();
            n += TypeSize.INTEGER * 3;
            n += TypeSize.SHORT;
            for (IndexItem item : items) {
                n += item.getStoredLength();
            }
            return n;
        }

        @Override
        public final void store(ByteBuffer bb) {
            super.store(bb);
            bb.putInt(rightSibling);
            bb.putInt(leftSibling);
            bb.putInt(targetSibling);
            bb.putShort((short) items.size());
            for (IndexItem item : items) {
                item.store(bb);
            }
        }

        public final PageId[] getPageIds() {
            return new PageId[] { getPageId(),
                    new PageId(getPageId().getContainerId(), rightSibling) };
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("NewRedistributeOperation(leftSibling=").append(
                    leftSibling);
            sb.append(", rightSibling=").append(rightSibling);
            sb.append(", targetSibling=").append(targetSibling);
            sb.append("No of items=").append(items.size()).append(newline);
            for (IndexItem item : items) {
                sb.append("  item #=[");
                item.appendTo(sb);
                sb.append("]").append(newline);
            }
            sb.append(", PageId[]={");
            for (PageId pageId : getPageIds()) {
                pageId.appendTo(sb);
                sb.append(",");
            }
            sb.append("})");
            return sb;
        }

        @Override
        public final String toString() {
            return appendTo(new StringBuilder()).toString();
        }

        static final class NewRedistributeOperationFactory implements
                ObjectFactory {
            private final ObjectRegistry objectRegistry;

            NewRedistributeOperationFactory(ObjectRegistry objectRegistry) {
                this.objectRegistry = objectRegistry;
            }

            public Class<?> getType() {
                return NewRedistributeOperation.class;
            }

            public Object newInstance(ByteBuffer buf) {
                return new NewRedistributeOperation(objectRegistry, buf);
            }
        }
    }

    /**
     * Log record for IncreaseTreeHeight operation. Must be logged as part of
     * the root page update. The log is applied to the root page and the new
     * child page. It is defined as a Compensation record so that it can be
     * linked back in such a way that if this operation completes, it is treated
     * as a nested top action.
     * 
     * @see BTreeImpl#doIncreaseTreeHeight(Transaction, BTreeContext)
     * @see BTreeIndexManagerImpl#redoIncreaseTreeHeightOperation(Page,
     *      IncreaseTreeHeightOperation)
     */
    public static final class IncreaseTreeHeightOperation extends
            BTreeLogOperation implements Compensation, MultiPageRedo {

        /**
         * These items that will become part of the new left child page.
         * Includes the highkey in leaf pages.
         */
        LinkedList<IndexItem> items;

        /**
         * Root page will contain 2 index entries. First will point to left
         * child, while the second will point to right child.
         */
        LinkedList<IndexItem> rootItems;

        /**
         * New left child page.
         */
        int leftSibling;

        /**
         * Right child page.
         */
        int rightSibling;

        /**
         * Owner of the newly allocated left sibling (left child) page.
         */
        int spaceMapPageNumber;

        public IncreaseTreeHeightOperation(ObjectRegistry objectRegistry,
                ByteBuffer bb) {
            super(objectRegistry, bb);
            short numberOfItems = bb.getShort();
            items = new LinkedList<IndexItem>();
            for (int i = 0; i < numberOfItems; i++) {
                IndexItem item = makeNewItem(bb);
                items.add(item);
            }
            numberOfItems = bb.getShort();
            rootItems = new LinkedList<IndexItem>();
            for (int i = 0; i < numberOfItems; i++) {
                IndexItem item = makeNewItem(false, isUnique(), bb);
                rootItems.add(item);
            }
            leftSibling = bb.getInt();
            rightSibling = bb.getInt();
            spaceMapPageNumber = bb.getInt();
        }

        public IncreaseTreeHeightOperation(int moduleId, int typeCode,
                ObjectRegistry objectRegistry, int keyFactoryType,
                int locationFactoryType, boolean leaf, boolean unique) {
            super(moduleId, typeCode, objectRegistry, keyFactoryType,
                    locationFactoryType, leaf, unique);
            items = new LinkedList<IndexItem>();
            rootItems = new LinkedList<IndexItem>();
        }

        @Override
        public final int getStoredLength() {
            int n = super.getStoredLength();
            n += TypeSize.SHORT;
            for (IndexItem item : items) {
                n += item.getStoredLength();
            }
            n += TypeSize.SHORT;
            for (IndexItem item : rootItems) {
                n += item.getStoredLength();
            }
            n += TypeSize.INTEGER * 3;
            return n;
        }

        @Override
        public final void store(ByteBuffer bb) {
            super.store(bb);
            bb.putShort((short) items.size());
            for (IndexItem item : items) {
                item.store(bb);
            }
            bb.putShort((short) rootItems.size());
            for (IndexItem item : rootItems) {
                item.store(bb);
            }
            bb.putInt(leftSibling);
            bb.putInt(rightSibling);
            bb.putInt(spaceMapPageNumber);
        }

        /**
         * This log record will be applioed to the root page and its newly
         * allocated left child.
         */
        public final PageId[] getPageIds() {
            return new PageId[] { getPageId(),
                    new PageId(getPageId().getContainerId(), leftSibling) };
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("IncreaseTreeHeightOperation(");
            sb.append(newline).append("New leaf node:").append(newline);
            sb.append("No of items=").append(items.size()).append(newline);
            for (IndexItem item : items) {
                sb.append("  item #=[");
                item.appendTo(sb);
                sb.append("]").append(newline);
            }
            sb.append("New root node:").append(newline);
            sb.append("No of items=").append(rootItems.size()).append(newline);
            for (IndexItem item : rootItems) {
                sb.append("  item #=[");
                item.appendTo(sb);
                sb.append("]").append(newline);
            }
            sb.append(", leftSibling=").append(leftSibling);
            sb.append(", rightSibling=").append(rightSibling);
            sb.append(", spaceMapPageNumber=").append(spaceMapPageNumber);
            sb.append(", PageId[]={");
            for (PageId pageId : getPageIds()) {
                pageId.appendTo(sb);
                sb.append(",");
            }
            sb.append("})");
            return sb;
        }

        @Override
        public final String toString() {
            return appendTo(new StringBuilder()).toString();
        }

        static final class IncreaseTreeHeightOperationFactory implements
                ObjectFactory {
            private final ObjectRegistry objectRegistry;

            IncreaseTreeHeightOperationFactory(ObjectRegistry objectRegistry) {
                this.objectRegistry = objectRegistry;
            }

            public Class<?> getType() {
                return IncreaseTreeHeightOperation.class;
            }

            public Object newInstance(ByteBuffer buf) {
                return new IncreaseTreeHeightOperation(objectRegistry, buf);
            }
        }

    }

    /**
     * Decrease of the height of the tree when root page has only one child.
     * Must be logged as part of the root page update.
     * 
     * @see BTreeIndexManagerImpl.BTreeImpl#doDecreaseTreeHeight(Transaction,
     *      BTreeContext)
     * @see BTreeIndexManagerImpl#redoDecreaseTreeHeightOperation(Page,
     *      DecreaseTreeHeightOperation)
     */
    public static final class DecreaseTreeHeightOperation extends
            BTreeLogOperation implements Redoable, MultiPageRedo {

        /**
         * The items that will become part of the new root page - copied over
         * from child page. Includes the highkey in leaf pages.
         */
        LinkedList<IndexItem> items;

        /**
         * Child page to be deallocated
         */
        int childPageNumber;

        /**
         * Space map page that owns the child page data
         */
        int childPageSpaceMap;

        public DecreaseTreeHeightOperation(ObjectRegistry objectRegistry,
                ByteBuffer bb) {
            super(objectRegistry, bb);
            short numberOfItems = bb.getShort();
            items = new LinkedList<IndexItem>();
            for (int i = 0; i < numberOfItems; i++) {
                IndexItem item = makeNewItem(bb);
                items.add(item);
            }
            childPageNumber = bb.getInt();
            childPageSpaceMap = bb.getInt();
        }

        public DecreaseTreeHeightOperation(int moduleId, int typeCode,
                ObjectRegistry objectRegistry, int keyFactoryType,
                int locationFactoryType, boolean leaf, boolean unique) {
            super(moduleId, typeCode, objectRegistry, keyFactoryType,
                    locationFactoryType, leaf, unique);
            items = new LinkedList<IndexItem>();
        }

        @Override
        public final int getStoredLength() {
            int n = super.getStoredLength();
            n += TypeSize.SHORT;
            for (IndexItem item : items) {
                n += item.getStoredLength();
            }
            n += TypeSize.INTEGER * 2;
            return n;
        }

        @Override
        public final void store(ByteBuffer bb) {
            super.store(bb);
            bb.putShort((short) items.size());
            for (IndexItem item : items) {
                item.store(bb);
            }
            bb.putInt(childPageNumber);
            bb.putInt(childPageSpaceMap);
        }

        /**
         * This log record will be applied to the root page, and the child page
         * that is to be deallocated. It can also be applied to the space map
         * page, but we handle the space map page update separately in the
         * interest of high concurrency.
         */
        public final PageId[] getPageIds() {
            //			return new PageId[] { getPageId(), new PageId(getPageId().getContainerId(), childPageNumber),
            //					new PageId(getPageId().getContainerId(), childPageSpaceMap) };
            return new PageId[] { getPageId(),
                    new PageId(getPageId().getContainerId(), childPageNumber) };
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("DecreaseTreeHeightOperation(");
            sb.append(newline);
            sb.append("No of items=").append(items.size()).append(newline);
            for (IndexItem item : items) {
                sb.append("  item #=[");
                item.appendTo(sb);
                sb.append("]").append(newline);
            }
            sb.append(", childPageNumber=").append(childPageNumber);
            sb.append(", childPageSpaceMap=").append(childPageSpaceMap);
            sb.append(", PageId[]={");
            for (PageId pageId : getPageIds()) {
                pageId.appendTo(sb);
                sb.append(",");
            }
            sb.append("})");
            return sb;
        }

        @Override
        public final String toString() {
            return appendTo(new StringBuilder()).toString();
        }

        static final class DecreaseTreeHeightOperationFactory implements
                ObjectFactory {
            private final ObjectRegistry objectRegistry;

            DecreaseTreeHeightOperationFactory(ObjectRegistry objectRegistry) {
                this.objectRegistry = objectRegistry;
            }

            public Class<?> getType() {
                return DecreaseTreeHeightOperation.class;
            }

            public Object newInstance(ByteBuffer buf) {
                return new DecreaseTreeHeightOperation(objectRegistry, buf);
            }
        }
    }

    /**
     * PartialIndexItem is useful when we need to instantiate an item only to
     * check the child page pointer. In such situations, it is expensive to
     * instantiate the whole item; instead we can do it cheaper by instantiating
     * a PartialIndexItem.
     * 
     * @author Dibyendu Majumdar
     * @since 08 Aug 08
     */
    static class PartialIndexItem implements Storable {

        /**
         * Pointer to child node that has keys &lt;= this key. This is an optional
         * field; only present in index pages.
         */
        protected int childPageNumber;

        /**
         * A non-persistent flag.
         */
        protected boolean isLeaf;

        PartialIndexItem(PartialIndexItem other) {
            this.isLeaf = other.isLeaf;
            this.childPageNumber = other.childPageNumber;
        }

        PartialIndexItem(boolean isLeaf, int childPageNumber) {
            this.isLeaf = isLeaf;
            this.childPageNumber = childPageNumber;
        }

        public PartialIndexItem(boolean isLeaf, ByteBuffer buf) {
            this.isLeaf = isLeaf;
            if (!isLeaf) {
                childPageNumber = buf.getInt();
            } else {
                childPageNumber = -1;
            }
        }

        /* (non-Javadoc)
         * @see org.simpledbm.io.Storable#store(java.nio.ByteBuffer)
         */
        public void store(ByteBuffer bb) {
            if (!isLeaf) {
                bb.putInt(childPageNumber);
            }
        }

        /* (non-Javadoc)
         * @see org.simpledbm.io.Storable#getStoredLength()
         */
        public int getStoredLength() {
            int len = 0;
            if (!isLeaf) {
                len += TypeSize.INTEGER;
            }
            return len;
        }

        static final class PartialIndexItemFactory implements StorableFactory {

            final boolean isleaf;

            PartialIndexItemFactory(boolean isleaf) {
                this.isleaf = isleaf;
            }

            public Storable getStorable(ByteBuffer buf) {
                return new PartialIndexItem(isleaf, buf);
            }

        }
    }

    /**
     * IndexItem represents an item within a BTree Page. Both Index pages and
     * Leaf pages contain IndexItems. However, the content of IndexItem is
     * somewhat different in Index pages than the content in Leaf pages.
     * <p>
     * In Index pages, an item contains a key, and a child page pointer, plus a
     * location, if the index is non-unique.
     * <p>
     * In leaf pages, an item contains a key and a location.
     * 
     * @author Dibyendu Majumdar
     * @since 18-Sep-2005
     */
    public static final class IndexItem extends PartialIndexItem implements
            Storable, Comparable<IndexItem>, Dumpable {

        /**
         * Sortable key
         */
        private final IndexKey key;

        /**
         * Location is an optional field; only present in leaf pages and in
         * non-unique index pages.
         */
        private final Location location;

        /**
         * A non-persistent flag.
         */
        private boolean isUnique;

        /**
         * Location is an optional field; used if the item is part of a
         * non-unique index or if the item belongs to a leaf page.
         */
        private boolean isLocationRequired() {
            return !isUnique || isLeaf;
        }

        public IndexItem(IndexKey key, Location loc, int childPageNumber,
                boolean isLeaf, boolean isUnique) {
            super(isLeaf, childPageNumber);
            this.key = key;
            this.location = loc;
            this.isUnique = isUnique;
        }

        public IndexItem(IndexItem other) {
            super(other);
            this.isUnique = other.isUnique;
            if (other.key != null) {
                this.key = other.key.cloneIndexKey();
            } else {
                this.key = null;
            }
            this.location = other.location;
        }

        public IndexItem(boolean isLeaf, boolean isUnique, int containerId,
                IndexKeyFactory keyFactory, LocationFactory locationFactory,
                ByteBuffer bb) {
            super(isLeaf, bb);
            this.isUnique = isUnique;
            key = keyFactory.newIndexKey(containerId, bb);
            if (isLocationRequired()) {
                location = locationFactory.newLocation(bb);
            } else {
                location = null;
            }
        }

        /* (non-Javadoc)
         * @see org.simpledbm.io.Storable#store(java.nio.ByteBuffer)
         */
        public final void store(ByteBuffer bb) {
            super.store(bb);
            key.store(bb);
            if (isLocationRequired()) {
                location.store(bb);
            }
        }

        /* (non-Javadoc)
         * @see org.simpledbm.io.Storable#getStoredLength()
         */
        public final int getStoredLength() {
            int len = super.getStoredLength();
            len += key.getStoredLength();
            if (isLocationRequired()) {
                len += location.getStoredLength();
            }
            return len;
        }

        public final int compareTo(
                org.simpledbm.rss.impl.im.btree.BTreeIndexManagerImpl.IndexItem o) {
            int comp = key.compareTo(o.key);
            //            if (comp == 0 && isLocationRequired() && o.isLocationRequired()) {                
            if (comp == 0 && !isUnique() && !o.isUnique()) {
                return location.compareTo(o.location);
            }
            return comp;
        }

        public final int compareToIgnoreLocation(
                org.simpledbm.rss.impl.im.btree.BTreeIndexManagerImpl.IndexItem o) {
            return key.compareTo(o.key);
        }

        public final int getChildPageNumber() {
            return childPageNumber;
        }

        public final void setChildPageNumber(int childPageNumber) {
            this.childPageNumber = childPageNumber;
        }

        public final boolean isLeaf() {
            return isLeaf;
        }

        public final void setLeaf(boolean isLeaf) {
            this.isLeaf = isLeaf;
        }

        public final boolean isUnique() {
            return isUnique;
        }

        public final void setUnique(boolean isUnique) {
            this.isUnique = isUnique;
        }

        public final IndexKey getKey() {
            return key;
        }

        public final Location getLocation() {
            return location;
        }

        @Override
        public int hashCode() {
            final int PRIME = 31;
            int result = 1;
            result = PRIME * result + childPageNumber;
            result = PRIME * result + (isLeaf ? 1231 : 1237);
            result = PRIME * result + (isUnique ? 1231 : 1237);
            result = PRIME * result + ((key == null) ? 0 : key.hashCode());
            result = PRIME * result
                    + ((location == null) ? 0 : location.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            final IndexItem other = (IndexItem) obj;
            return compareTo(other) == 0;
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("IndexItem(key=[").append(key).append("], isLeaf=")
                    .append(isLeaf).append(", isUnique=").append(isUnique);
            if (isLocationRequired()) {
                sb.append(", Location=").append(location);
            }
            if (!isLeaf()) {
                sb.append(", ChildPage=").append(childPageNumber);
            }
            sb.append(")");
            return sb;
        }

        @Override
        public final String toString() {
            return appendTo(new StringBuilder()).toString();
        }
    }

    /**
     * Represents the redo log data for initializing a single BTree node (page)
     * and the corresponding space map page.
     * 
     * @see BTreeIndexManagerImpl#redoLoadPageOperation(Page, LoadPageOperation)
     * @see XMLLoader
     */
    public static final class LoadPageOperation extends BTreeLogOperation
            implements Redoable, MultiPageRedo {

        /**
         * The IndexItems that will become part of the new page.
         */
        LinkedList<IndexItem> items;

        /**
         * Pointer to the sibling node to the left.
         */
        int leftSibling;

        /**
         * Pointer to the sibling node to the right.
         */
        int rightSibling;

        /**
         * Space Map page that should be updated to reflect the allocated status
         * of the new page.
         */
        private int spaceMapPageNumber;

        public LoadPageOperation(ObjectRegistry objectRegistry, ByteBuffer bb) {
            super(objectRegistry, bb);
            short numberOfItems = bb.getShort();
            items = new LinkedList<IndexItem>();
            for (int i = 0; i < numberOfItems; i++) {
                IndexItem item = makeNewItem(bb);
                items.add(item);
            }
            leftSibling = bb.getInt();
            rightSibling = bb.getInt();
            setSpaceMapPageNumber(bb.getInt());
        }

        public LoadPageOperation(int moduleId, int typeCode,
                ObjectRegistry objectRegistry, int keyFactoryType,
                int locationFactoryType, boolean leaf, boolean unique) {
            super(moduleId, typeCode, objectRegistry, keyFactoryType,
                    locationFactoryType, leaf, unique);
            items = new LinkedList<IndexItem>();
        }

        @Override
        public final int getStoredLength() {
            int n = super.getStoredLength();
            n += TypeSize.SHORT;
            for (IndexItem item : items) {
                n += item.getStoredLength();
            }
            n += TypeSize.INTEGER * 3;
            return n;
        }

        @Override
        public final void store(ByteBuffer bb) {
            super.store(bb);
            bb.putShort((short) items.size());
            for (IndexItem item : items) {
                item.store(bb);
            }
            bb.putInt(leftSibling);
            bb.putInt(rightSibling);
            bb.putInt(getSpaceMapPageNumber());
        }

        public final PageId[] getPageIds() {
            return new PageId[] {
                    getPageId(),
                    new PageId(getPageId().getContainerId(),
                            getSpaceMapPageNumber()) };
        }

        void setSpaceMapPageNumber(int spaceMapPageNumber) {
            this.spaceMapPageNumber = spaceMapPageNumber;
        }

        int getSpaceMapPageNumber() {
            return spaceMapPageNumber;
        }

        @Override
        public final String toString() {
            return "LoadPageOperation(" + super.toString() + "leftSibling="
                    + leftSibling + ", rightSibling=" + rightSibling
                    + "spaceMapPageNumber=" + getSpaceMapPageNumber()
                    + ", numberOfItems=" + items.size() + ")";
        }

        static final class LoadPageOperationFactory implements ObjectFactory {
            private final ObjectRegistry objectRegistry;

            LoadPageOperationFactory(ObjectRegistry objectRegistry) {
                this.objectRegistry = objectRegistry;
            }

            public Class<?> getType() {
                return LoadPageOperation.class;
            }

            public Object newInstance(ByteBuffer buf) {
                return new LoadPageOperation(objectRegistry, buf);
            }
        }

    }

    static final class LoadPageOperationInput {

        ObjectRegistry objectRegistry;

        int moduleId = BTreeIndexManagerImpl.MODULE_ID;

        int typeCode = BTreeIndexManagerImpl.TYPE_LOADPAGE_OPERATION;

        int keyFactoryType;

        int locationFactoryType;

        IndexKeyFactory keyFactory;

        LocationFactory locationFactory;

        /**
         * The IndexItems that will become part of the new page.
         */
        LinkedList<IndexItem> items = new LinkedList<IndexItem>();

        /**
         * Pointer to the sibling node to the left.
         */
        int leftSibling;

        /**
         * Pointer to the sibling node to the right.
         */
        int rightSibling;

        /**
         * Space Map page that should be updated to reflect the allocated status
         * of the new page.
         */
        int spaceMapPageNumber;

        int pageType;

        PageId pageId;

        boolean leaf;

        boolean unique;

        public void setKeyFactoryType(int keyFactoryType) {
            this.keyFactoryType = keyFactoryType;
            this.keyFactory = (IndexKeyFactory) objectRegistry
                    .getSingleton(keyFactoryType);
        }

        public void setLocationFactoryType(int locationFactoryType) {
            this.locationFactoryType = locationFactoryType;
            this.locationFactory = (LocationFactory) objectRegistry
                    .getSingleton(locationFactoryType);
        }

        IndexKey getNewIndexKey(String s) {
            return keyFactory.parseIndexKey(pageId.getContainerId(), s);
        }
    }

    /**
     * Helper class that reads an XML document and creates LoadPageOperation
     * records using the data in the document. Primary purspose is to help with
     * testing of the BTree algorithms by preparing trees with specific
     * characteristics.
     */
    public static final class XMLLoader {

        final BTreeIndexManagerImpl btreemgr;

        ArrayList<LoadPageOperation> records = new ArrayList<LoadPageOperation>();

        public XMLLoader(BTreeIndexManagerImpl btreemgr) {
            this.btreemgr = btreemgr;
        }

        public final void parseResource(String filename) throws Exception {
            DocumentBuilderFactory factory = DocumentBuilderFactory
                    .newInstance();
            try {
                DocumentBuilder builder = factory.newDocumentBuilder();
                Document document = builder.parse(btreemgr.po.getClassUtils()
                        .getResourceAsStream(filename));
                loadDocument(document);
            } catch (SAXException sxe) {
                Exception x = sxe;
                if (sxe.getException() != null)
                    x = sxe.getException();
                throw x;
            }
        }

        public final ArrayList<LoadPageOperation> getPageOperations() {
            return records;
        }

        private void loadDocument(Document document) throws Exception {
            NodeList nodelist = document.getChildNodes();

            for (int x = 0; x < nodelist.getLength(); x++) {
                Node rootnode = nodelist.item(x);
                if (rootnode.getNodeType() != Node.ELEMENT_NODE
                        || !rootnode.getNodeName().equals("tree")) {
                    continue;
                }
                NodeList nodes = rootnode.getChildNodes();

                for (int i = 0; i < nodes.getLength(); i++) {
                    Node node = nodes.item(i);
                    if (node.getNodeType() == Node.ELEMENT_NODE
                            && node.getNodeName().equals("page")) {
                        LoadPageOperationInput loadPageOpInput = new LoadPageOperationInput();
                        loadPageOpInput.objectRegistry = btreemgr.objectFactory;
                        loadPage(loadPageOpInput, node);

                        LoadPageOperation loadPageOp = new LoadPageOperation(
                                BTreeIndexManagerImpl.MODULE_ID,
                                BTreeIndexManagerImpl.TYPE_LOADPAGE_OPERATION,
                                btreemgr.objectFactory,
                                loadPageOpInput.keyFactoryType,
                                loadPageOpInput.locationFactoryType,
                                loadPageOpInput.leaf, loadPageOpInput.unique);
                        loadPageOp.setPageId(loadPageOpInput.pageType,
                                loadPageOpInput.pageId);
                        loadPageOp.items = loadPageOpInput.items;
                        loadPageOp.leftSibling = loadPageOpInput.leftSibling;
                        loadPageOp.rightSibling = loadPageOpInput.rightSibling;
                        loadPageOp.spaceMapPageNumber = loadPageOpInput.spaceMapPageNumber;

                        records.add(loadPageOp);
                    }
                }
                break;
            }
        }

        private void loadPage(LoadPageOperationInput loadPageOp, Node page)
                throws Exception {
            int containerId = -1;
            int pageNumber = -1;
            NamedNodeMap attrs = page.getAttributes();
            if (attrs != null) {
                Node n = attrs.getNamedItem("containerId");
                if (n != null) {
                    containerId = Integer.parseInt(n.getTextContent());
                }
                n = attrs.getNamedItem("pageNumber");
                if (n != null) {
                    pageNumber = Integer.parseInt(n.getTextContent());
                }
            }
            if (containerId == -1 || pageNumber == -1) {
                throw new Exception(
                        "page element must have containerId and pageNumber attributes");
            }
            loadPageOp.pageType = btreemgr.spMgr.getPageType();
            loadPageOp.pageId = new PageId(containerId, pageNumber);
            NodeList nodes = page.getChildNodes();
            for (int i = 0; i < nodes.getLength(); i++) {
                Node node = nodes.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE
                        && node.getNodeName().equals("header")) {
                    loadHeader(loadPageOp, node);
                } else if (node.getNodeType() == Node.ELEMENT_NODE
                        && node.getNodeName().equals("items")) {
                    NodeList itemsList = node.getChildNodes();
                    for (int j = 0; j < itemsList.getLength(); j++) {
                        Node itemNode = itemsList.item(j);
                        if (itemNode.getNodeType() == Node.ELEMENT_NODE
                                && itemNode.getNodeName().equals("item")) {
                            loadItem(loadPageOp, itemNode);
                        }
                    }
                }
            }
        }

        private void loadItem(LoadPageOperationInput loadPageOp, Node item)
                throws Exception {
            String keyValue = null;
            int childPageNumber = -1;
            String locationValue = null;

            NodeList nodes = item.getChildNodes();
            for (int i = 0; i < nodes.getLength(); i++) {
                Node node = nodes.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE
                        && node.getNodeName().equals("childpagenumber")) {
                    childPageNumber = Integer.parseInt(node.getTextContent());
                } else if (node.getNodeType() == Node.ELEMENT_NODE
                        && node.getNodeName().equals("location")) {
                    locationValue = node.getTextContent();
                } else if (node.getNodeType() == Node.ELEMENT_NODE
                        && node.getNodeName().equals("key")) {
                    keyValue = node.getTextContent();
                }
            }

            IndexKey key = loadPageOp.getNewIndexKey(keyValue);
            Location location = loadPageOp.locationFactory
                    .newLocation(locationValue);
            loadPageOp.items.add(new IndexItem(key, location, childPageNumber,
                    loadPageOp.leaf, loadPageOp.unique));
        }

        private void loadHeader(LoadPageOperationInput loadPageOp, Node header)
                throws Exception {
            NodeList nodes = header.getChildNodes();
            for (int i = 0; i < nodes.getLength(); i++) {
                Node node = nodes.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE
                        && node.getNodeName().equals("unique")) {
                    String value = node.getTextContent();
                    loadPageOp.unique = value.equals("yes");
                } else if (node.getNodeType() == Node.ELEMENT_NODE
                        && node.getNodeName().equals("leaf")) {
                    String value = node.getTextContent();
                    loadPageOp.leaf = value.equals("yes");
                } else if (node.getNodeType() == Node.ELEMENT_NODE
                        && node.getNodeName().equals("leftsibling")) {
                    String value = node.getTextContent();
                    loadPageOp.leftSibling = Integer.parseInt(value);
                } else if (node.getNodeType() == Node.ELEMENT_NODE
                        && node.getNodeName().equals("rightsibling")) {
                    String value = node.getTextContent();
                    loadPageOp.rightSibling = Integer.parseInt(value);
                } else if (node.getNodeType() == Node.ELEMENT_NODE
                        && node.getNodeName().equals("smppagenumber")) {
                    String value = node.getTextContent();
                    loadPageOp.spaceMapPageNumber = Integer.parseInt(value);
                } else if (node.getNodeType() == Node.ELEMENT_NODE
                        && node.getNodeName().equals("locationfactory")) {
                    String value = node.getTextContent();
                    loadPageOp.setLocationFactoryType(Short.parseShort(value));
                } else if (node.getNodeType() == Node.ELEMENT_NODE
                        && node.getNodeName().equals("keyfactory")) {
                    String value = node.getTextContent();
                    loadPageOp.setKeyFactoryType(Short.parseShort(value));
                }
            }
        }
    }

    public static final int getTestingFlag() {
        return testingFlag;
    }

    public static final void setTestingFlag(int testingFlag) {
        BTreeIndexManagerImpl.testingFlag = testingFlag;
    }
}
