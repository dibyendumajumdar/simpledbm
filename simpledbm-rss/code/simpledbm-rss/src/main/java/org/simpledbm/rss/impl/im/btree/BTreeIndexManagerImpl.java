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
package org.simpledbm.rss.impl.im.btree;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.simpledbm.rss.api.bm.BufferAccessBlock;
import org.simpledbm.rss.api.bm.BufferManager;
import org.simpledbm.rss.api.exception.RSSException;
import org.simpledbm.rss.api.fsm.FreeSpaceChecker;
import org.simpledbm.rss.api.fsm.FreeSpaceCursor;
import org.simpledbm.rss.api.fsm.FreeSpaceManager;
import org.simpledbm.rss.api.fsm.FreeSpaceMapPage;
import org.simpledbm.rss.api.im.IndexContainer;
import org.simpledbm.rss.api.im.IndexException;
import org.simpledbm.rss.api.im.IndexKey;
import org.simpledbm.rss.api.im.IndexKeyFactory;
import org.simpledbm.rss.api.im.IndexManager;
import org.simpledbm.rss.api.im.IndexScan;
import org.simpledbm.rss.api.im.UniqueConstraintViolationException;
import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.loc.LocationFactory;
import org.simpledbm.rss.api.locking.LockDuration;
import org.simpledbm.rss.api.locking.LockException;
import org.simpledbm.rss.api.locking.LockMode;
import org.simpledbm.rss.api.locking.util.LockAdaptor;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.registry.ObjectRegistry;
import org.simpledbm.rss.api.registry.ObjectRegistryAware;
import org.simpledbm.rss.api.sp.SlottedPage;
import org.simpledbm.rss.api.sp.SlottedPageManager;
import org.simpledbm.rss.api.st.Storable;
import org.simpledbm.rss.api.tx.BaseLoggable;
import org.simpledbm.rss.api.tx.BaseTransactionalModule;
import org.simpledbm.rss.api.tx.Compensation;
import org.simpledbm.rss.api.tx.IsolationMode;
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
import org.simpledbm.rss.tools.diagnostics.Trace;
import org.simpledbm.rss.util.ClassUtils;
import org.simpledbm.rss.util.Dumpable;
import org.simpledbm.rss.util.TypeSize;
import org.simpledbm.rss.util.logging.DiagnosticLogger;
import org.simpledbm.rss.util.logging.Logger;
import org.simpledbm.rss.util.mcat.MessageCatalog;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * B-link Tree implementation is based upon the algorithms described in <cite>
 * Ibrahim Jaluta, Seppo Sippu and Eljas Soisalon-Soininen. Concurrency control 
 * and recovery for balanced B-link trees. The VLDB Journal, Volume 14, Issue 2 
 * (April 2005), Pages: 257 - 277, ISSN:1066-8888.</cite> There are some variations
 * from the published algorithms - these are noted at appropriate places within the
 * code. 
 * <p>
 * <h2>Structure of the B-link Tree</h2>
 * <ol>
 * <li>The tree is contained in a container of fixed size pages. The
 * first page (pagenumber = 0) of the container is a header page. The second page (pagenumber = 1) is the
 * first space map page. The third page (pagenumber = 2) is always allocated as 
 * the root page of the tree. The root page never changes.
 * </li>
 * <li>Pages at all levels are linked to their right siblings.</li>
 * <li>In leaf pages, an extra item called the high key is present. In index pages,
 * the last key acts as the highkey. All keys in a page are guaranteed to be &lt;= 
 * than the highkey. Note that in leaf pages the highkey may not be the same as
 * the last key in the page.</li>
 * <li>In index pages, each key contains a pointer to a child page. The child page contains
 * keys &lt;= to the key in the index page. The highkey of the child page will match the
 * index key if the child is a direct child. The highkey of the child page will be &lt; than
 * the index key if the child has a sibling that is an indirect child.
 * </li>
 * <li>All pages other than root must have at least two items (excluding highkey in leaf pages). </li>
 * <li>The rightmost key at any level is a special key containing logical INFINITY. Initially, the empty
 * tree contains this key only. As the tree grows through splitting of pages, the INFINITY key is carried
 * forward to the rightmost pages at each level of the tree. This key can never be deleted from the
 * tree.</li>
 * <li>A page is considered as safe if it is not about to underflow and is not about to overflow.
 * A page is about to underflow if it is the root node and has 1 child or it is any other node and 
 * has two keys. A page is about to overflow if it cannot accomodate a new key.</li>
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
     * 2. When copying
     * keys from one page to another it is not necessary to instantiate keys. A
     * more efficient way would be to copy raw data. Current method is
     * inefficient.
     */

    static final String LOG_CLASS_NAME = BTreeIndexManagerImpl.class.getName();
    static final Logger log = Logger.getLogger(BTreeIndexManagerImpl.class
        .getPackage()
        .getName());

    static final MessageCatalog mcat = new MessageCatalog();
    
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

    /**
     * Space map value for a used BTree page. The space map can have two possible values - 1 means used, 
     * and 0 means unused.
     */
    private static final int PAGE_SPACE_UNUSED = 0;

    /**
     * Space map value for a used BTree page. The space map can have two possible values - 1 means used, 
     * and 0 means unused.
     */
    private static final int PAGE_SPACE_USED = 1;

    /**
     * Root page is always the third page in a container.
     */
    private static final int ROOT_PAGE_NUMBER = 2;

    /**
     * The first slot in a btree node page is occupied by a special
     * header record.
     */
    private static final int HEADER_KEY_POS = 0;

    /**
     * The keys start at slot position 1, because the
     * first slot is occupied by a header record.
     */
    private static final int FIRST_KEY_POS = 1;

    final ObjectRegistry objectFactory;

    final LoggableFactory loggableFactory;

    final FreeSpaceManager spaceMgr;

    final BufferManager bufmgr;

    final SlottedPageManager spMgr;

    final LockAdaptor lockAdaptor;

    /**
     * Setting this flag to 1 artificially limits the number
     * of keys in each page. This is useful for forcing page splits etc.
     * when testing.
     * @see BTreeNode#canAccomodate(org.simpledbm.rss.impl.im.btree.BTreeIndexManagerImpl.IndexItem)
     * @see BTreeNode#canMergeWith(org.simpledbm.rss.impl.im.btree.BTreeIndexManagerImpl.BTreeNode)
     * @see BTreeNode#getSplitKey()
     */
    private static int testingFlag = 0;

    public static final int TEST_MODE_LIMIT_MAX_KEYS_PER_PAGE = 1;

    /**
     * During test mode, force a node to have a maximum of 8 keys.
     * This causes more frequent page splits and merges.
     */
    private static final int TEST_MODE_MAX_KEYS = 8;

    /**
     * During test mode, the median key is defined as the 5th key (4).
     */
    private static final int TEST_MODE_SPLIT_KEY = 4;
	private static final boolean Validating = false;
	private static final boolean QuickValidate = false;

    public BTreeIndexManagerImpl(ObjectRegistry objectFactory,
            LoggableFactory loggableFactory, FreeSpaceManager spaceMgr,
            BufferManager bufMgr, SlottedPageManager spMgr,
            TransactionalModuleRegistry moduleRegistry,
            LockAdaptor lockAdaptor, Properties p) {
        this.objectFactory = objectFactory;
        this.loggableFactory = loggableFactory;
        this.spaceMgr = spaceMgr;
        this.bufmgr = bufMgr;
        this.spMgr = spMgr;
        //this.lockAdaptor = new DefaultLockAdaptor();
        this.lockAdaptor = lockAdaptor;
        moduleRegistry.registerModule(MODULE_ID, this);

        objectFactory.registerType(TYPE_SPLIT_OPERATION, SplitOperation.class
            .getName());
        objectFactory.registerType(TYPE_MERGE_OPERATION, MergeOperation.class
            .getName());
        objectFactory.registerType(TYPE_LINK_OPERATION, LinkOperation.class
            .getName());
        objectFactory.registerType(TYPE_UNLINK_OPERATION, UnlinkOperation.class
            .getName());
        objectFactory.registerType(
            TYPE_REDISTRIBUTE_OPERATION,
            RedistributeOperation.class.getName());
        objectFactory.registerType(
            TYPE_INCREASETREEHEIGHT_OPERATION,
            IncreaseTreeHeightOperation.class.getName());
        objectFactory.registerType(
            TYPE_DECREASETREEHEIGHT_OPERATION,
            DecreaseTreeHeightOperation.class.getName());
        objectFactory.registerType(TYPE_INSERT_OPERATION, InsertOperation.class
            .getName());
        objectFactory.registerType(
            TYPE_UNDOINSERT_OPERATION,
            UndoInsertOperation.class.getName());
        objectFactory.registerType(TYPE_DELETE_OPERATION, DeleteOperation.class
            .getName());
        objectFactory.registerType(
            TYPE_UNDODELETE_OPERATION,
            UndoDeleteOperation.class.getName());
        objectFactory.registerType(
            TYPE_LOADPAGE_OPERATION,
            LoadPageOperation.class.getName());
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
        } else if (loggable instanceof IncreaseTreeHeightOperation) {
            redoIncreaseTreeHeightOperation(
                page,
                (IncreaseTreeHeightOperation) loggable);
        } else if (loggable instanceof DecreaseTreeHeightOperation) {
            redoDecreaseTreeHeightOperation(
                page,
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
     * Redo a LoadPageOperation. A LoadPageOperation is used to log the actions of
     * the XMLLoader which reads an XML file and generates a B-Tree. It is also used when 
     * initializing a new BTree.
     * 
     * @see #createIndex(Transaction, String, int, int, int, int, boolean)
     * @see XMLLoader 
     */
    private void redoLoadPageOperation(Page page, LoadPageOperation loadPageOp) {
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
            smp.setSpaceBits(
                loadPageOp.getPageId().getPageNumber(),
                PAGE_SPACE_USED);
        } else if (page.getPageId().getPageNumber() == loadPageOp
            .getPageId()
            .getPageNumber()) {
            /*
             * Log record is being applied to BTree page.
             */
            SlottedPage r = (SlottedPage) page;
            r.init();
            formatPage(r, loadPageOp.getKeyFactoryType(), loadPageOp
                .getLocationFactoryType(), loadPageOp.isLeaf(), loadPageOp
                .isUnique());
            r.setSpaceMapPageNumber(loadPageOp.getSpaceMapPageNumber());
            BTreeNode node = new BTreeNode(loadPageOp);
            node.wrap(r);
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
     * @see BTreeImpl#doSplit(Transaction, org.simpledbm.rss.impl.im.btree.BTreeIndexManagerImpl.BTreeCursor)
     * @see SplitOperation
     */
    private void redoSplitOperation(Page page, SplitOperation splitOperation) {
        if (page.getPageId().equals(splitOperation.getPageId())) {
        	Trace.event(0, page.getPageId().getContainerId(), page.getPageId().getPageNumber());
            // q is the page to be split
            SlottedPage q = (SlottedPage) page;
            BTreeNode leftSibling = new BTreeNode(splitOperation);
            leftSibling.wrap(q);
            if (Validating) {
            	leftSibling.validate();
            }
            leftSibling.header.rightSibling = splitOperation.newSiblingPageNumber;
            leftSibling.header.keyCount = splitOperation.newKeyCount;
            // get rid of the keys that will move to the
            // sibling page prior to updating the high key
            while (q.getNumberOfSlots() > leftSibling.header.keyCount + 1) {
                q.purge(q.getNumberOfSlots() - 1);
            }
            if (splitOperation.isLeaf()) {
                // update the high key
                leftSibling.replace(
                    splitOperation.newKeyCount,
                    splitOperation.highKey);
                assert leftSibling
                    .getItem(splitOperation.newKeyCount)
                    .compareTo(splitOperation.highKey) == 0;
            }
            leftSibling.updateHeader();
            if (Validating) {
            	leftSibling.validate();
            }
            leftSibling.dump();
        } else {
        	Trace.event(1, page.getPageId().getContainerId(), page.getPageId().getPageNumber());
            // r is the newly allocated right sibling of q
            SlottedPage r = (SlottedPage) page;
            r.init();
            formatPage(
                r,
                splitOperation.getKeyFactoryType(),
                splitOperation.getLocationFactoryType(),
                splitOperation.isLeaf(),
                splitOperation.isUnique());
            r.setSpaceMapPageNumber(splitOperation.spaceMapPageNumber);
            BTreeNode newSiblingNode = new BTreeNode(splitOperation);
            newSiblingNode.wrap(r);
            newSiblingNode.header.leftSibling = splitOperation
                .getPageId()
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
     * @see BTreeImpl#doMerge(Transaction, org.simpledbm.rss.impl.im.btree.BTreeIndexManagerImpl.BTreeCursor) 
     * @see MergeOperation
     */
    private void redoMergeOperation(Page page, MergeOperation mergeOperation) {
        if (page.getPageId().getPageNumber() == mergeOperation.rightSiblingSpaceMapPage) {
        	Trace.event(2, page.getPageId().getContainerId(), page.getPageId().getPageNumber());
            FreeSpaceMapPage smp = (FreeSpaceMapPage) page;
            // deallocate page by marking it unused
            smp.setSpaceBits(mergeOperation.rightSibling, PAGE_SPACE_UNUSED);
        } else if (page.getPageId().getPageNumber() == mergeOperation
            .getPageId()
            .getPageNumber()) {
        	Trace.event(3, page.getPageId().getContainerId(), page.getPageId().getPageNumber());
            // left sibling - this page will aborb contents of right sibling.
            SlottedPage q = (SlottedPage) page;
            BTreeNode leftSibling = new BTreeNode(mergeOperation);
            leftSibling.wrap(q);
            if (Validating) {
            	leftSibling.validate();
            }
            int k;
            int prevHighKey = leftSibling.header.keyCount;
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
            //if (Validating) {
            //	leftSibling.validate();
            //}
            if (Validating) {
            	try {
            		leftSibling.validate();
            	}
            	catch (RSSException e) {
            		System.err.println("==============================");
            		System.err.println("Previous high key = " + prevHighKey);
            		System.err.println("MergeOperation = " + mergeOperation);
            		throw e;
            	}
            }
            leftSibling.dump();
        } else if (page.getPageId().getPageNumber() == mergeOperation.rightSibling) {
            // mark right sibling as deallocated
        	Trace.event(4, page.getPageId().getContainerId(), page.getPageId().getPageNumber());
            SlottedPage p = (SlottedPage) page;
            short flags = p.getFlags();
            p.setFlags((short) (flags | BTreeNode.NODE_TREE_DEALLOCATED));
        }
    }

    /**
     * Redo a link operation
     * @see BTreeImpl#doLink(Transaction, org.simpledbm.rss.impl.im.btree.BTreeIndexManagerImpl.BTreeCursor)
     * @see LinkOperation
     */
    private void redoLinkOperation(Page page, LinkOperation linkOperation) {
    	Trace.event(5, linkOperation.rightSibling, page.getPageId().getContainerId(), page.getPageId().getPageNumber());
        SlottedPage p = (SlottedPage) page;
        BTreeNode parent = new BTreeNode(linkOperation);
        parent.wrap(p);
        if (Validating) {
        	parent.validate();
        }
        int k = 0;
        for (k = FIRST_KEY_POS; k <= parent.header.keyCount; k++) {
            IndexItem item = parent.getItem(k);
            // Change the index entry of left child to point to right child
            if (item.getChildPageNumber() == linkOperation.leftSibling) {
                item.setChildPageNumber(linkOperation.rightSibling);
                p.insertAt(k, item, true);
                break;
            }
        }
        if (k > parent.header.keyCount) {
            // child pointer not found - corrupt b-tree?
            log.error(LOG_CLASS_NAME, "redoLinkOperation", mcat
                .getMessage("EB0001"));
            throw new IndexException(mcat.getMessage("EB0001"));
        }
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
     * @see BTreeImpl#doUnlink(Transaction, BTreeCursor)
     * @see UnlinkOperation 
     */
    private void redoUnlinkOperation(Page page, UnlinkOperation unlinkOperation) {
    	Trace.event(6, unlinkOperation.rightSibling, page.getPageId().getContainerId(), page.getPageId().getPageNumber());
        SlottedPage p = (SlottedPage) page;
        BTreeNode parent = new BTreeNode(unlinkOperation);
        parent.wrap(p);
        if (Validating) {
        	parent.validate();
        }
        int k = 0;
        for (k = FIRST_KEY_POS; k <= parent.header.keyCount; k++) {
            IndexItem item = parent.getItem(k);
            if (item.getChildPageNumber() == unlinkOperation.leftSibling) {
                break;
            }
        }
        if (k > parent.header.keyCount) {
            log.error(LOG_CLASS_NAME, "redoUnlinkOperation", mcat
                .getMessage("EB0001"));
            throw new IndexException(mcat.getMessage("EB0001"));
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
            log.error(LOG_CLASS_NAME, "redoUnlinkOperation", mcat
                .getMessage("EB0001"));
            throw new IndexException(mcat.getMessage("EB0001"));
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
     * @see BTreeImpl#doRedistribute(Transaction, BTreeCursor)
     * @see RedistributeOperation
     */
    private void redoRedistributeOperation(Page page,
            RedistributeOperation redistributeOperation) {
        SlottedPage p = (SlottedPage) page;
        BTreeNode node = new BTreeNode(redistributeOperation);
        node.wrap(p);
        if (Validating) {
        	node.validate();
        }
        if (page.getPageId().getPageNumber() == redistributeOperation.leftSibling) {
            // processing Q
            if (redistributeOperation.targetSibling == redistributeOperation.leftSibling) {
                // moving key left
                // the new key will become the high key
                // FIXME Test case
            	Trace.event(7, page.getPageId().getContainerId(), page.getPageId().getPageNumber());
                node.header.keyCount = node.header.keyCount + 1;
                node.updateHeader();
                p.insertAt(
                    node.header.keyCount,
                    redistributeOperation.key,
                    true);
                if (redistributeOperation.isLeaf()) {
                    p.insertAt(
                        node.header.keyCount - 1,
                        redistributeOperation.key,
                        true);
                }
            } else {
                // moving key right
                // delete current high key
            	Trace.event(8, page.getPageId().getContainerId(), page.getPageId().getPageNumber());
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
                // FIXME Test case
            	Trace.event(9, page.getPageId().getContainerId(), page.getPageId().getPageNumber());
                p.purge(FIRST_KEY_POS);
                node.header.keyCount = node.header.keyCount - 1;
                node.updateHeader();
            } else {
                // moving key right
                // insert new key at position 1
            	Trace.event(10, page.getPageId().getContainerId(), page.getPageId().getPageNumber());
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
     * Redo an increase tree height operation.
     * @see BTreeImpl#doIncreaseTreeHeight(Transaction, BTreeCursor)
     * @see IncreaseTreeHeightOperation 
     */
    private void redoIncreaseTreeHeightOperation(Page page,
            IncreaseTreeHeightOperation ithOperation) {
        SlottedPage p = (SlottedPage) page;
        BTreeNode node = new BTreeNode(ithOperation);
        if (p.getPageId().equals(ithOperation.getPageId())) {
        	Trace.event(11, page.getPageId().getContainerId(), page.getPageId().getPageNumber());
            // root page
            // get rid of existing entries by reinitializing page
            int savedPageNumber = p.getSpaceMapPageNumber();
            p.init();
            p.setSpaceMapPageNumber(savedPageNumber);
            formatPage(p, ithOperation.getKeyFactoryType(), ithOperation
                .getLocationFactoryType(), false, ithOperation.isUnique());
            node.wrap(p);
            node.insert(1, ithOperation.rootItems.get(0));
            node.insert(2, ithOperation.rootItems.get(1));
            node.header.keyCount = 2;
            node.updateHeader();
        } else if (p.getPageId().getPageNumber() == ithOperation.leftSibling) {
        	Trace.event(12, page.getPageId().getContainerId(), page.getPageId().getPageNumber());
            // new left sibling
            p.init();
            formatPage(p, ithOperation.getKeyFactoryType(), ithOperation
                .getLocationFactoryType(), ithOperation.isLeaf(), ithOperation
                .isUnique());
            p.setSpaceMapPageNumber(ithOperation.spaceMapPageNumber);
            node.wrap(p);
            int k = FIRST_KEY_POS; // 0 is position of header item
            for (IndexItem item : ithOperation.items) {
                node.replace(k++, item);
            }
            node.header.rightSibling = ithOperation.rightSibling;
            node.header.keyCount = ithOperation.items.size();
            node.updateHeader();
        }
        if (Validating) {
        	node.validate();
        }
        node.dump();
    }

    /**
     * Decrease tree height when root page has only one child and that child does not
     * have a sibling.
     * @see BTreeImpl#doDecreaseTreeHeight(org.simpledbm.rss.api.tx.Transaction, org.simpledbm.rss.impl.im.btree.BTreeIndexManagerImpl.BTreeCursor)
     * @see DecreaseTreeHeightOperation
     */
    private void redoDecreaseTreeHeightOperation(Page page,
            DecreaseTreeHeightOperation dthOperation) {
        if (page.getPageId().getPageNumber() == dthOperation.childPageSpaceMap) {
            // This is not executed if the space map is updated
            // as a separate action. But we leave this code here in case we 
            // wish to update the space map as part of the same action.
            // FIXME TEST case
            FreeSpaceMapPage smp = (FreeSpaceMapPage) page;
            // deallocate
            smp.setSpaceBits(dthOperation.childPageNumber, PAGE_SPACE_UNUSED);
        } else if (page.getPageId().getPageNumber() == dthOperation
            .getPageId()
            .getPageNumber()) {
        	Trace.event(13, page.getPageId().getContainerId(), page.getPageId().getPageNumber());
            // root page 
            // delete contents and absorb contents of only child
            SlottedPage p = (SlottedPage) page;
            BTreeNode node = new BTreeNode(dthOperation);
            int savedPageNumber = p.getSpaceMapPageNumber();
            p.init();
            p.setSpaceMapPageNumber(savedPageNumber);
            // Leaf page status must be replicated from child page
            formatPage(p, dthOperation.getKeyFactoryType(), dthOperation
                .getLocationFactoryType(), dthOperation.isLeaf(), dthOperation
                .isUnique());
            node.wrap(p);
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
        	Trace.event(14, page.getPageId().getContainerId(), page.getPageId().getPageNumber());
            SlottedPage p = (SlottedPage) page;
            short flags = p.getFlags();
            p.setFlags((short) (flags | BTreeNode.NODE_TREE_DEALLOCATED));
        }
    }

    /**
     * Performs an insert operation on the leaf page. 
     * @param page Page where the insert should take place
     * @param insertOp The log record containing details of the insert operation
     * @see BTreeImpl#doInsert(Transaction, IndexKey, Location)
     * @see BTreeImpl#doInsertTraverse(Transaction, org.simpledbm.rss.impl.im.btree.BTreeIndexManagerImpl.BTreeCursor)
     */
    private void redoInsertOperation(Page page, InsertOperation insertOp) {
    	Trace.event(15, page.getPageId().getContainerId(), page.getPageId().getPageNumber());
        SlottedPage p = (SlottedPage) page;
        BTreeNode node = new BTreeNode(insertOp);
        node.wrap(p);
        if (Validating) {
        	node.validate();
        }
        SearchResult sr = node.search(insertOp.getItem());
        assert !sr.exactMatch;
        if (sr.k == -1) {
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
     * @param page The page where the insert should be undone.
     * @param undoInsertOp The log record containing information on the undo operation.
     */
    private void redoUndoInsertOperation(Page page,
            UndoInsertOperation undoInsertOp) {
    	Trace.event(16, page.getPageId().getContainerId(), page.getPageId().getPageNumber());
        SlottedPage p = (SlottedPage) page;
        BTreeNode node = new BTreeNode(undoInsertOp);
        node.wrap(p);
        if (Validating) {
        	node.validate();
        }
        if (QuickValidate) {
        	assert node.getItem(undoInsertOp.getPosition()).equals(undoInsertOp.getItem());
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
     * Performs a logical undo of a key insert. Checks if the page
     * that originally contained the key is still the right page. If not,
     * traverses the tree to find the correct page.
     * @param trx The transaction handling this operation
     * @param insertOp The insert operation that will be undone.
     */
    private void undoInsertOperation(Transaction trx, InsertOperation insertOp) {
//		Undo-insert(T,P,k,m) { X-latch(P);
//		if (P still contains r and will not underflow if r is deleted) { Q = P;
//		} else { unlatch(P); update-mode-traverse(k,Q);
//		upgrade-latch(Q);
//		}delete r from Q;
//		log(n, <T, undo-insert, Q, k, m>);
//		Page-LSN(Q) = n; Undo-Next-LSN(T) = m; unlatch(Q);
//		}

        BTreeCursor bcursor = new BTreeCursor(this);
    	Trace.event(17, insertOp.getPageId().getContainerId(), insertOp.getPageId().getPageNumber());
        bcursor.setP(bufmgr.fixExclusive(insertOp.getPageId(), false, -1, 0));
        try {
            SlottedPage p = (SlottedPage) bcursor.getP().getPage();
            BTreeNode node = new BTreeNode(insertOp);
            node.wrap(p);
            if (Validating) {
            	node.validate();
            }
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
                doSearch = true;
            } else {
            	Trace.event(18, p.getPageId().getContainerId(), p.getPageId().getPageNumber());
                sr = node.search(insertOp.getItem());
                if (sr.exactMatch && node.header.keyCount > node.minimumKeys()) {
                    /*
                     * Page still contains the key and will not underflow if the key
                     * is deleted
                     */
                    doSearch = false;
                } else {
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
            	Trace.event(19);
                bcursor.unfixP();
                BTreeImpl btree = getBTreeImpl(insertOp
                    .getPageId()
                    .getContainerId(), insertOp.getKeyFactoryType(), insertOp
                    .getLocationFactoryType(), insertOp.isUnique());
                bcursor.setSearchKey(insertOp.getItem());
                btree.updateModeTraverse(trx, bcursor);
                /* At this point p points to the leaf page where the key is present */
                assert bcursor.getP() != null;
                assert bcursor.getP().isLatchedForUpdate();
            	Trace.event(20, bcursor.getP().getPage().getPageId().getContainerId(), bcursor.getP().getPage().getPageId().getPageNumber());
                bcursor.getP().upgradeUpdateLatch();
                p = (SlottedPage) bcursor.getP().getPage();
                node = new BTreeNode(insertOp);
                node.wrap(p);
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
            UndoInsertOperation undoInsertOp = (UndoInsertOperation) loggableFactory
                .getInstance(
                    MODULE_ID,
                    BTreeIndexManagerImpl.TYPE_UNDOINSERT_OPERATION);
            undoInsertOp.copyFrom(insertOp);
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
     * @param page Page where the key is to be deleted from
     * @param deleteOp The log operation describing the delete
     */
    private void redoDeleteOperation(Page page, DeleteOperation deleteOp) {
    	Trace.event(21, page.getPageId().getContainerId(), page.getPageId().getPageNumber());
    	SlottedPage p = (SlottedPage) page;
        BTreeNode node = new BTreeNode(deleteOp);
        node.wrap(p);
        if (Validating) {
        	node.validate();
        }
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
     * Redo a undo operation on a key delete on the specified leaf page.
     * @param page Page where the deleted key will be restored
     * @param undoDeleteOp The log operation describing the undo operation.
     */
    private void redoUndoDeleteOperation(Page page,
            UndoDeleteOperation undoDeleteOp) {
    	Trace.event(22, page.getPageId().getContainerId(), page.getPageId().getPageNumber());
        SlottedPage p = (SlottedPage) page;
        BTreeNode node = new BTreeNode(undoDeleteOp);
        node.wrap(p);
        if (Validating) {
        	node.validate();
        }
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
     * Perform a logical undo operation. If the page containing the original 
     * key is no longer the right page to perform an undo, then the BTree will be
     * traversed to locate the right page.
     * 
     * @param trx The transaction constrolling this operation
     * @param deleteOp The log record that describes the delete that will be undone.
     */
    private void undoDeleteOperation(Transaction trx, DeleteOperation deleteOp) {
//		X-latch(P);
//		if (P still covers r and there is a room for r in P) { Q = P;
//		} else { unlatch(P); update-mode-traverse(k,Q);
//		if (Q is full) split(Q);
//		upgrade-latch(Q);
//		} insert r into Q;
//		log(n, <T, undo-delete, Q, (k, x), m>);
//		Page-LSN(Q) = n; Undo-Next-LSN(T) = m; unlatch(Q);

        BTreeCursor bcursor = new BTreeCursor(this);
    	Trace.event(23, deleteOp.getPageId().getContainerId(), deleteOp.getPageId().getPageNumber());
        bcursor.setP(bufmgr.fixExclusive(deleteOp.getPageId(), false, -1, 0));
        try {
            SlottedPage p = (SlottedPage) bcursor.getP().getPage();
            BTreeNode node = new BTreeNode(deleteOp);
            node.wrap(p);
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
                if (Validating) {
                	node.validate();
                }
            } else {
                /*
                 * We need to traverse the tree to find the leaf page where the key
                 * now lives.
                 */
            	Trace.event(24);
                bcursor.unfixP();
                BTreeImpl btree = getBTreeImpl(deleteOp
                    .getPageId()
                    .getContainerId(), deleteOp.getKeyFactoryType(), deleteOp
                    .getLocationFactoryType(), deleteOp.isUnique());
                bcursor.setSearchKey(deleteOp.getItem());
                btree.updateModeTraverse(trx, bcursor);
                /* At this point p points to the leaf page where the key should be inserted */
                assert bcursor.getP() != null;
                assert bcursor.getP().isLatchedForUpdate();
                p = (SlottedPage) bcursor.getP().getPage();
                node.wrap(p);
                if (!node.canAccomodate(bcursor.searchKey)) {
                    bcursor.setQ(bcursor.removeP());
                    btree.doSplit(trx, bcursor);
                    bcursor.setP(bcursor.removeQ());
                }
                assert bcursor.getP().isLatchedForUpdate();
                Trace.event(25, bcursor.getP().getPage().getPageId().getContainerId(), bcursor.getP().getPage().getPageId().getPageNumber());
                bcursor.getP().upgradeUpdateLatch();
                p = (SlottedPage) bcursor.getP().getPage();
                node.wrap(p);
                assert node.isLeaf();
                assert !node.isDeallocated();
            }
            SearchResult sr = node.search(deleteOp.getItem());
            assert !sr.exactMatch;
            if (sr.k == -1) {
                // this is the rightmost key in this node
                assert node.getHighKey().compareTo(deleteOp.getItem()) >= 0;
                sr.k = node.header.keyCount;
            }
            assert sr.k != -1;

            /*
             * Now we can reinsert the key that was deleted. First generate 
             * a Compensation Log Record.
             */
            UndoDeleteOperation undoDeleteOp = (UndoDeleteOperation) loggableFactory
                .getInstance(
                    MODULE_ID,
                    BTreeIndexManagerImpl.TYPE_UNDODELETE_OPERATION);
            undoDeleteOp.copyFrom(deleteOp);
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
        return new BTreeImpl(
            this,
            containerId,
            keyFactoryType,
            locationFactoryType,
            unique);
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
            BTreeNodeHeader header = new BTreeNodeHeader();
            page.get(HEADER_KEY_POS, header);
            keyFactoryType = header.getKeyFactoryType();
            locationFactoryType = header.getLocationFactoryType();
        } finally {
            bab.unfix();
        }
        return getBTreeImpl(
            containerId,
            keyFactoryType,
            locationFactoryType,
            unique);
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.im.IndexManager#getIndex(org.simpledbm.rss.api.tx.Transaction, int)
     */
    public IndexContainer getIndex(Transaction trx, int containerId) {
        trx.acquireLock(
            lockAdaptor.getLockableContainerId(containerId),
            LockMode.SHARED,
            LockDuration.COMMIT_DURATION);
        return getIndex(containerId);
    }
    
    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.im.IndexManager#lockIndexContainer(org.simpledbm.rss.api.tx.Transaction, int, org.simpledbm.rss.api.locking.LockMode)
     */
    public void lockIndexContainer(Transaction trx, int containerId, LockMode mode) {
        trx.acquireLock(
            lockAdaptor.getLockableContainerId(containerId),
            mode,
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

            trx.acquireLock(
                lockAdaptor.getLockableContainerId(containerId),
                LockMode.EXCLUSIVE,
                LockDuration.COMMIT_DURATION);
            /*
             * Create the specified container
             */
            spaceMgr.createContainer(
                trx,
                name,
                containerId,
                1,
                extentSize,
                spMgr.getPageType());

            PageId pageid = new PageId(containerId, ROOT_PAGE_NUMBER);
            /*
             * Initialize the root page, and update space map page. 
             */
            LoadPageOperation loadPageOp = (LoadPageOperation) loggableFactory
                .getInstance(MODULE_ID, TYPE_LOADPAGE_OPERATION);
            loadPageOp.setUnique(unique);
            loadPageOp.setLeaf(true);
            loadPageOp.setKeyFactoryType(keyFactoryType);
            loadPageOp.setLocationFactoryType(locationFactoryType);
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
                PageId spaceMapPageId = new PageId(
                    pageid.getContainerId(),
                    loadPageOp.getSpaceMapPageNumber());
                BufferAccessBlock smpBab = bufmgr.fixExclusive(
                    spaceMapPageId,
                    false,
                    -1,
                    0);
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

    /**
     * Formats a new BTree page.
     */
    static void formatPage(SlottedPage page, int keyFactoryType,
            int locationFactoryType, boolean leaf, boolean isUnique) {
        /*
         * FIXME: This method has knowledge of the BTreeNode and BTreeNodeHeader structures.
         * Ideally this ought to be encapsulated in BTreeNode
         */
        short flags = 0;
        if (leaf) {
            flags |= BTreeNode.NODE_TYPE_LEAF;
        }
        if (isUnique) {
            flags |= BTreeNode.NODE_TREE_UNIQUE;
        }
        page.setFlags(flags);
        BTreeNodeHeader header = new BTreeNodeHeader();
        header.setKeyFactoryType(keyFactoryType);
        header.setLocationFactoryType(locationFactoryType);
        page.insertAt(HEADER_KEY_POS, header, true);
    }

    static interface IndexItemHelper {
        public Location getNewLocation();

        public IndexKey getNewIndexKey();

        public IndexKey getMaxIndexKey();
        
        public boolean isUnique();
    }

    public static final class BTreeImpl implements IndexItemHelper,
            IndexContainer {
        public final FreeSpaceCursor spaceCursor;
        final BTreeIndexManagerImpl btreeMgr;
        final int containerId;

        final int keyFactoryType;
        final int locationFactoryType;

        final IndexKeyFactory keyFactory;
        final LocationFactory locationFactory;

        boolean unique;

        BTreeImpl(BTreeIndexManagerImpl btreeMgr, int containerId,
                int keyFactoryType, int locationFactoryType, boolean unique) {
            this.btreeMgr = btreeMgr;
            this.containerId = containerId;
            this.keyFactoryType = keyFactoryType;
            this.locationFactoryType = locationFactoryType;
            this.keyFactory = (IndexKeyFactory) btreeMgr.objectFactory
                .getInstance(keyFactoryType);
            this.locationFactory = (LocationFactory) btreeMgr.objectFactory
                .getInstance(locationFactoryType);
            this.unique = unique;
            spaceCursor = btreeMgr.spaceMgr.getSpaceCursor(containerId);
        }
        
        public final boolean isUnique() {
            return unique;
        }

        public final IndexKey getNewIndexKey() {
            return keyFactory.newIndexKey(containerId);
        }

        public final IndexKey getMaxIndexKey() {
            return keyFactory.maxIndexKey(containerId);
        }

        public final Location getNewLocation() {
            return locationFactory.newLocation();
        }

        public final BTreeNode getBTreeNode() {
            return new BTreeNode(this);
        }

        public final void setUnique(boolean unique) {
            this.unique = unique;
        }

        /**
         * Performs page split. The page to be split (bcursor.q) must be latched in 
         * UPDATE mode prior to the call. After the split,
         * the page containing the search key will remain latched as bcursor.q.
         * <p>
         * This differs from the published algorithm in following ways:
         * 1. It uses nested top action. 
         * 2. Page allocation is logged as redo-undo.
         * 3. Space map page latch is released prior to any other exclusive latch.
         * 4. The split is logged as Compensation record, with undoNextLsn set to the LSN prior to the page allocation log record.
         * 5. Information about space map page is stored in new page.
         * 
         * @see SplitOperation
         * @param trx Transaction managing the page split operation
         * @param bcursor bcursor.q must be the page that is to be split.
         */
        public final void doSplit(Transaction trx, BTreeCursor bcursor) {

        	assert bcursor.getQ().isLatchedForUpdate();
        	
        	final BTreeImpl btree = this;

            Lsn undoNextLsn = null;

            int newSiblingPageNumber = btree.spaceCursor
                .findAndFixSpaceMapPageExclusively(new SpaceCheckerImpl());
            int spaceMapPageNumber = -1;
            try {
                if (newSiblingPageNumber == -1) {
                	Trace.event(26);
                    btree.btreeMgr.spaceMgr.extendContainer(
                        trx,
                        btree.containerId);
                    undoNextLsn = trx.getLastLsn();
                    newSiblingPageNumber = btree.spaceCursor
                        .findAndFixSpaceMapPageExclusively(new SpaceCheckerImpl());
                    if (newSiblingPageNumber == -1) {
                        log.error(LOG_CLASS_NAME, "doSplit", mcat
                            .getMessage("EB0002"));
                        throw new IndexException(mcat.getMessage("EB0002"));
                    }
                }
                undoNextLsn = trx.getLastLsn();
                btree.spaceCursor.updateAndLogUndoably(
                    trx,
                    newSiblingPageNumber,
                    PAGE_SPACE_USED);
                spaceMapPageNumber = btree.spaceCursor
                    .getCurrentSpaceMapPage()
                    .getPageId()
                    .getPageNumber();
            } finally {
                if (newSiblingPageNumber != -1) {
                    btree.spaceCursor.unfixCurrentSpaceMapPage();
                }
            }

            BTreeNode leftSiblingNode = btree.getBTreeNode();
            Trace.event(27, bcursor.getQ().getPage().getPageId().getContainerId(), bcursor.getQ().getPage().getPageId().getPageNumber());
            bcursor.getQ().upgradeUpdateLatch();
            assert bcursor.getQ().isLatchedExclusively();
            leftSiblingNode.wrap((SlottedPage) bcursor.getQ().getPage());

            PageId newSiblingPageId = new PageId(
                btree.containerId,
                newSiblingPageNumber);
            SplitOperation splitOperation = (SplitOperation) btree.btreeMgr.loggableFactory
                .getInstance(
                    BTreeIndexManagerImpl.MODULE_ID,
                    BTreeIndexManagerImpl.TYPE_SPLIT_OPERATION);
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
            splitOperation.setKeyFactoryType(btree.keyFactoryType);
            splitOperation.setLocationFactoryType(btree.locationFactoryType);
            splitOperation.newSiblingPageNumber = newSiblingPageNumber;
            splitOperation.rightSibling = leftSiblingNode.header.rightSibling;
            splitOperation.spaceMapPageNumber = spaceMapPageNumber;
            splitOperation.setLeaf(leftSiblingNode.isLeaf());
            splitOperation.setUnique(btree.isUnique());
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

            Trace.event(28, newSiblingPageId.getContainerId(), newSiblingPageId.getPageNumber());
            bcursor.setR(btree.btreeMgr.bufmgr.fixExclusive(
                newSiblingPageId,
                true,
                btree.btreeMgr.spMgr.getPageType(),
                0));

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
                	Trace.event(29, bcursor.getQ().getPage().getPageId().getContainerId(), bcursor.getQ().getPage().getPageId().getPageNumber());
                    bcursor.getQ().downgradeExclusiveLatch();
                	assert bcursor.getQ().isLatchedForUpdate();
                    bcursor.unfixR();
                } else {
                    // new key will be in the right sibling
                	Trace.event(30, bcursor.getR().getPage().getPageId().getContainerId(), bcursor.getR().getPage().getPageId().getPageNumber());
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
         * Merges right sibling into left sibling. Right sibling must be an indirect child.
         * Both pages must be latched in UPDATE mode prior to this call.
         * After the merge, left sibling will remain latched as bcursor.q. 
         * <p>
         * This algorithm differs from published algorithm in its management of space map
         * update. In the interests of high concurrency, the space map page update is
         * handled as a separate redo only action. 
         */
        public final void doMerge(Transaction trx, BTreeCursor bcursor) {

            final BTreeImpl btree = this;

            assert bcursor.getR() != null;
            assert bcursor.getR().isLatchedForUpdate();

            assert bcursor.getQ() != null;
            assert bcursor.getQ().isLatchedForUpdate();

            BTreeNode leftSiblingNode = btree.getBTreeNode();
            Trace.event(31, bcursor.getQ().getPage().getPageId().getContainerId(), bcursor.getQ().getPage().getPageId().getPageNumber());
            bcursor.getQ().upgradeUpdateLatch();
            leftSiblingNode.wrap((SlottedPage) bcursor.getQ().getPage());

            Trace.event(32, bcursor.getR().getPage().getPageId().getContainerId(), bcursor.getR().getPage().getPageId().getPageNumber());
            bcursor.getR().upgradeUpdateLatch();
            BTreeNode rnode = btree.getBTreeNode();
            rnode.wrap((SlottedPage) bcursor.getR().getPage());

            assert leftSiblingNode.header.rightSibling == rnode.page
                .getPageId()
                .getPageNumber();

//			SlottedPage rpage = (SlottedPage) bcursor.getR().getPage();
//			PageId spaceMapPageId = new PageId(btree.containerId, rpage.getSpaceMapPageNumber()); 

//			BufferAccessBlock smpBab = btree.btreeMgr.bufmgr.fixExclusive(spaceMapPageId, false, "", 0);

            MergeOperation mergeOperation = (MergeOperation) btree.btreeMgr.loggableFactory
                .getInstance(
                    BTreeIndexManagerImpl.MODULE_ID,
                    BTreeIndexManagerImpl.TYPE_MERGE_OPERATION);

            mergeOperation.setLeaf(leftSiblingNode.isLeaf());
            mergeOperation.setUnique(btree.isUnique());
            mergeOperation.setKeyFactoryType(btree.keyFactoryType);
            mergeOperation.setLocationFactoryType(btree.locationFactoryType);
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

                Trace.event(33, bcursor.getQ().getPage().getPageId().getContainerId(), bcursor.getQ().getPage().getPageId().getPageNumber());
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
                btree.spaceCursor.updateAndLogRedoOnly(
                    trx,
                    mergeOperation.rightSibling,
                    0);
            } finally {
                btree.spaceCursor.unfixCurrentSpaceMapPage();
            }
        }

        /**
         * Link the right sibling to the parent, when the right sibling is an 
         * indirect child. Parent and left child must be latched in UPDATE
         * mode prior to invoking this method. Both will remain latched at the
         * end of the operation. 
         * <p>Note that this differs from published algorithm slightly:
         * <pre>
         * v = highkey of R
         * u = highkey of Q
         * Link(P, Q, R) {
         * 	upgrade-latch(P);
         * 	change the index record (v, Q.pageno) to (v, R.pageno);
         * 	insert the index record (u, Q.pageno) before (v, R.pageno);
         * 	lsn = log(<unlink, P, Q.pageno, R.pageno>);
         * 	P.pageLsn = lsn;
         * 	downgrade-latch(P);
         * }
         * </pre>
         */
        public final void doLink(Transaction trx, BTreeCursor bcursor) {

            final BTreeImpl btree = this;

            assert bcursor.getP() != null;
            assert bcursor.getP().isLatchedForUpdate();

            assert bcursor.getQ() != null;
            assert bcursor.getQ().isLatchedForUpdate();

            Trace.event(34, bcursor.getP().getPage().getPageId().getContainerId(), bcursor.getP().getPage().getPageId().getPageNumber());
            bcursor.getP().upgradeUpdateLatch();
            try {
				BTreeNode parentNode = btree.getBTreeNode();
				parentNode.wrap((SlottedPage) bcursor.getP().getPage());

				BTreeNode lnode = btree.getBTreeNode();
				lnode.wrap((SlottedPage) bcursor.getQ().getPage());
				SlottedPage lpage = (SlottedPage) bcursor.getQ().getPage();

				if (Validating) {
					assert parentNode.findIndexItem(lnode.header.rightSibling) == null;
				}
				
				LinkOperation linkOperation = (LinkOperation) btree.btreeMgr.loggableFactory
						.getInstance(BTreeIndexManagerImpl.MODULE_ID,
								BTreeIndexManagerImpl.TYPE_LINK_OPERATION);
				linkOperation.setLeaf(parentNode.isLeaf()); // should be false
				linkOperation.setUnique(btree.isUnique());
				linkOperation.setKeyFactoryType(btree.keyFactoryType);
				linkOperation.setLocationFactoryType(btree.locationFactoryType);
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
				Trace.event(35, bcursor.getP().getPage().getPageId().getContainerId(), bcursor.getP().getPage().getPageId().getPageNumber());
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
         * <p>Note that this differs from published algorithm slightly:
         * <pre>
         * v = highkey of R
         * u = highkey of Q
         * Unlink(P, Q, R) {
         * 	upgrade-latch(P);
         * 	delete the index record (u, Q.pageno);
         * 	change the index record (v, R.pageno) to (v, Q.pageno);
         * 	lsn = log(<unlink, P, Q.pageno, R.pageno>);
         * 	P.pageLsn = lsn;
         * 	unfix(P);
         * }
         * </pre>
         */
        public final void doUnlink(Transaction trx, BTreeCursor bcursor) {

            final BTreeImpl btree = this;

            assert bcursor.getP() != null;
            assert bcursor.getP().isLatchedForUpdate();

            assert bcursor.getQ() != null;
            assert bcursor.getQ().isLatchedForUpdate();

            assert bcursor.getR() != null;
            assert bcursor.getR().isLatchedForUpdate();

            Trace.event(36, bcursor.getP().getPage().getPageId().getContainerId(), bcursor.getP().getPage().getPageId().getPageNumber());
            bcursor.getP().upgradeUpdateLatch();
            try {
				BTreeNode parentNode = btree.getBTreeNode();
				parentNode.wrap((SlottedPage) bcursor.getP().getPage());

				UnlinkOperation unlinkOperation = (UnlinkOperation) btree.btreeMgr.loggableFactory
						.getInstance(BTreeIndexManagerImpl.MODULE_ID,
								BTreeIndexManagerImpl.TYPE_UNLINK_OPERATION);
				unlinkOperation.setLeaf(parentNode.isLeaf()); // should be false
				unlinkOperation.setUnique(btree.isUnique());
				unlinkOperation.setKeyFactoryType(btree.keyFactoryType);
				unlinkOperation
						.setLocationFactoryType(btree.locationFactoryType);
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
         * Redistribute the keys between sibling nodes when the right child is an
         * indirect child of parent page. Both pages must be latched in UPDATE
         * mode prior to calling this method. At the end of this operation,
         * the child page that covers the search key will remain latched in
         * UPDATE mode. 
         * <p>
         * Unlike the published algorithm we simply transfer one key from the more 
         * densely populated page to the less populated page.
         * @param bcursor bcursor.q must point to left page, and bcursor.r to its right sibling
         */
        public final void doRedistribute(Transaction trx, BTreeCursor bcursor) {

            final BTreeImpl btree = this;

            assert bcursor.getQ() != null;
            assert bcursor.getQ().isLatchedForUpdate();

            assert bcursor.getR() != null;
            assert bcursor.getR().isLatchedForUpdate();

            assert bcursor.searchKey != null;

            Trace.event(37, bcursor.getQ().getPage().getPageId().getContainerId(), bcursor.getQ().getPage().getPageId().getPageNumber());
            bcursor.getQ().upgradeUpdateLatch();
            BTreeNode leftSiblingNode = btree.getBTreeNode();
            leftSiblingNode.wrap((SlottedPage) bcursor.getQ().getPage());

            Trace.event(38, bcursor.getR().getPage().getPageId().getContainerId(), bcursor.getR().getPage().getPageId().getPageNumber());
            bcursor.getR().upgradeUpdateLatch();
            BTreeNode rightSiblingNode = btree.getBTreeNode();
            rightSiblingNode.wrap((SlottedPage) bcursor.getR().getPage());
            SlottedPage rpage = (SlottedPage) bcursor.getR().getPage();

            RedistributeOperation redistributeOperation = (RedistributeOperation) btree.btreeMgr.loggableFactory
                .getInstance(
                    BTreeIndexManagerImpl.MODULE_ID,
                    BTreeIndexManagerImpl.TYPE_REDISTRIBUTE_OPERATION);
            redistributeOperation.setLeaf(leftSiblingNode.isLeaf());
            redistributeOperation.setUnique(btree.isUnique());
            redistributeOperation.setKeyFactoryType(btree.keyFactoryType);
            redistributeOperation
                .setLocationFactoryType(btree.locationFactoryType);
            redistributeOperation.leftSibling = leftSiblingNode.page
                .getPageId()
                .getPageNumber();
            redistributeOperation.rightSibling = rpage
                .getPageId()
                .getPageNumber();
            if (leftSiblingNode.page.getFreeSpace() > rpage.getFreeSpace()) {
                // key moving left
            	redistributeOperation.key = rightSiblingNode.getItem(FIRST_KEY_POS);
                redistributeOperation.targetSibling = redistributeOperation.leftSibling;
            } else {
                // key moving right
                redistributeOperation.key = leftSiblingNode.getLastKey();
                redistributeOperation.targetSibling = redistributeOperation.rightSibling;
            }

            try {
                Lsn lsn = trx.logInsert(
                    leftSiblingNode.page,
                    redistributeOperation);

                btree.btreeMgr
                    .redo(leftSiblingNode.page, redistributeOperation);
                bcursor.getQ().setDirty(lsn);

                btree.btreeMgr.redo(rpage, redistributeOperation);
                bcursor.getR().setDirty(lsn);

                leftSiblingNode.wrap((SlottedPage) bcursor.getQ().getPage());
                /* Check if Q covers the current search key value */
                int comp = leftSiblingNode.getHighKey().compareTo(
                    bcursor.searchKey);
                if (comp >= 0) {
                    // new key will stay in current page
                    // FIXME TEST case
                	Trace.event(39, bcursor.getQ().getPage().getPageId().getContainerId(), bcursor.getQ().getPage().getPageId().getPageNumber());
                    bcursor.getQ().downgradeExclusiveLatch();
                    bcursor.unfixR();
                } else {
                    // new key will be in the right sibling
                	Trace.event(40, bcursor.getR().getPage().getPageId().getContainerId(), bcursor.getR().getPage().getPageId().getPageNumber());
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
         * Increase tree height when root page as a sibling page. The root page and its
         * sibling must be latched in UPDATE mode prior to calling this method. After the
         * operation is complete, the latch on the root page is released, but one of the child
         * pages (the one that covers the search key) will be left latched in UPDATE mode.
         * <p>
         * The implementation differs from the published algorithm as follows:
         * <ol>
         * <li>
         * No need to format the new page, as this is taken care of in the space
         * management module. New pages are formatted as soon as they are created.
         * </li>
         * <li>
         * We use a nested top action to manage the entire action. This is to improve
         * concurrency, as it allows the space map page update to be completed before
         * any other page is latched exclusively. The SMO is logged as a Compensation
         * record and linked to the log record prior to the space map update. This makes the
         * SMO redoable, but the space map update will be undone if the SMO log does
         * not survive.
         * </li>
         * </ol>
         * @param bcursor bcursor.q must point to root page, and bcursor.r to its right sibling
         */
        public final void doIncreaseTreeHeight(Transaction trx,
                BTreeCursor bcursor) {

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
                	Trace.event(41);
                    btree.btreeMgr.spaceMgr.extendContainer(
                        trx,
                        btree.containerId);
                    newSiblingPageNumber = btree.spaceCursor
                        .findAndFixSpaceMapPageExclusively(new SpaceCheckerImpl());
                    if (newSiblingPageNumber == -1) {
                        log.error(LOG_CLASS_NAME, "doIncreaseTreeHeight", mcat
                            .getMessage("EB0002"));
                        throw new IndexException(mcat.getMessage("EB0002"));
                    }
                }
                // Make a note of current lsn so that we can link the Compensation record to it.
                undoNextLsn = trx.getLastLsn();
                btree.spaceCursor.updateAndLogUndoably(
                    trx,
                    newSiblingPageNumber,
                    PAGE_SPACE_USED);
                spaceMapPageNumber = btree.spaceCursor
                    .getCurrentSpaceMapPage()
                    .getPageId()
                    .getPageNumber();
            } finally {
                if (newSiblingPageNumber != -1) {
                    btree.spaceCursor.unfixCurrentSpaceMapPage();
                }
            }

            bcursor.setP(bcursor.removeQ());
            BTreeNode rootNode = btree.getBTreeNode();
            
            assert bcursor.getP().isLatchedForUpdate();
            Trace.event(42, bcursor.getP().getPage().getPageId().getContainerId(), bcursor.getP().getPage().getPageId().getPageNumber());
            bcursor.getP().upgradeUpdateLatch();
            rootNode.wrap((SlottedPage) bcursor.getP().getPage());

            IncreaseTreeHeightOperation ithOperation = (IncreaseTreeHeightOperation) btree.btreeMgr.loggableFactory
                .getInstance(
                    BTreeIndexManagerImpl.MODULE_ID,
                    BTreeIndexManagerImpl.TYPE_INCREASETREEHEIGHT_OPERATION);
            ithOperation.setUndoNextLsn(undoNextLsn);
            ithOperation.setKeyFactoryType(btree.keyFactoryType);
            ithOperation.setLocationFactoryType(btree.locationFactoryType);
            ithOperation.rightSibling = rootNode.header.rightSibling;
            ithOperation.leftSibling = newSiblingPageNumber;
            ithOperation.spaceMapPageNumber = spaceMapPageNumber;
            // New child page will inherit the root page leaf attribute
            ithOperation.setLeaf(rootNode.isLeaf());
            ithOperation.setUnique(btree.isUnique());
            for (int k = FIRST_KEY_POS; k <= rootNode.header.keyCount; k++) {
                ithOperation.items.add(rootNode.getItem(k));
            }
            IndexItem leftChildHighKey = rootNode
                .getItem(rootNode.header.keyCount);
            leftChildHighKey.setLeaf(false);
            leftChildHighKey.setChildPageNumber(ithOperation.leftSibling);
            IndexItem rightChildHighKey = rootNode.getInfiniteKey();
            rightChildHighKey.setLeaf(false);
            rightChildHighKey.setChildPageNumber(ithOperation.rightSibling);
            ithOperation.rootItems.add(leftChildHighKey);
            ithOperation.rootItems.add(rightChildHighKey);

            // Latch the new page exclusively
            PageId newChildPageId = new PageId(
                    btree.containerId,
                    ithOperation.leftSibling);
            Trace.event(43, newChildPageId.getContainerId(), newChildPageId.getPageNumber());
            bcursor.setQ(btree.btreeMgr.bufmgr.fixExclusive(newChildPageId, true, btree.btreeMgr.spMgr
                .getPageType(), 0));
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
                	Trace.event(44, bcursor.getQ().getPage().getPageId().getContainerId(), bcursor.getQ().getPage().getPageId().getPageNumber());
                    bcursor.getQ().downgradeExclusiveLatch();
                    bcursor.unfixR();
                } else {
                    // new key will be in the right child page
                	Trace.event(45, bcursor.getR().getPage().getPageId().getContainerId(), bcursor.getR().getPage().getPageId().getPageNumber());
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
         * Decrease tree height when root page has only one child and that child does not have 
         * a right sibling. The root page and its child must be latched in UPDATE mode prior
         * to calling this method. At the end of this operation, the root will remain latched in
         * UPDATE mode.
         * <p>
         * Important note:
         * To increase concurrency, we update the space map page after the SMO as a separate
         * redo only action. This improves concurrency because we do not hold the space map
         * page exclusively during the SMO. However, it has the disadvantage that if the SMO 
         * survives a system crash, and the log for the space map page updates does not survive,
         * then the page will remain allocated on the space map, even though it is no longer
         * in use. It is posible to identify deallocated pages by checking the page flags for the
         * bit BTreeNode.
         * @param bcursor bcursor.p must point to root page, and bcursor.q to only child
         */
        public final void doDecreaseTreeHeight(Transaction trx,
                BTreeCursor bcursor) {

            final BTreeImpl btree = this;

            assert bcursor.getP() != null;
            assert bcursor.getP().isLatchedForUpdate();

            assert bcursor.getQ() != null;
            assert bcursor.getQ().isLatchedForUpdate();

            // root page
            Trace.event(46, bcursor.getP().getPage().getPageId().getContainerId(), bcursor.getP().getPage().getPageId().getPageNumber());
            bcursor.getP().upgradeUpdateLatch();
            BTreeNode rootNode = btree.getBTreeNode();
            rootNode.wrap((SlottedPage) bcursor.getP().getPage());

            // child page
            Trace.event(47, bcursor.getQ().getPage().getPageId().getContainerId(), bcursor.getQ().getPage().getPageId().getPageNumber());
            bcursor.getQ().upgradeUpdateLatch();
            BTreeNode childNode = btree.getBTreeNode();
            childNode.wrap((SlottedPage) bcursor.getQ().getPage());

            DecreaseTreeHeightOperation dthOperation = (DecreaseTreeHeightOperation) btree.btreeMgr.loggableFactory
                .getInstance(
                    BTreeIndexManagerImpl.MODULE_ID,
                    BTreeIndexManagerImpl.TYPE_DECREASETREEHEIGHT_OPERATION);
            dthOperation.setKeyFactoryType(btree.keyFactoryType);
            dthOperation.setLocationFactoryType(btree.locationFactoryType);
            // root will inherit the leaf status of child node
            dthOperation.setLeaf(childNode.isLeaf());
            //dthOperation.setUnique(dthOperation.isUnique());
            dthOperation.setUnique(btree.isUnique());
            for (int k = FIRST_KEY_POS; k <= childNode.header.keyCount; k++) {
                dthOperation.items.add(childNode.getItem(k));
            }
            dthOperation.childPageSpaceMap = childNode.page
                .getSpaceMapPageNumber();
            dthOperation.childPageNumber = childNode.page
                .getPageId()
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
                Trace.event(48, bcursor.getP().getPage().getPageId().getContainerId(), bcursor.getP().getPage().getPageId().getPageNumber());
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
                btree.spaceCursor.updateAndLogRedoOnly(
                    trx,
                    dthOperation.childPageNumber,
                    0);
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
        final void doSplitParent(Transaction trx, BTreeCursor bcursor) {
            /*
             * doSplit requires Q to point to page that is to be
             * split, so we need to point Q to P temporarily.
             */
        	Trace.event(49, bcursor.getP().getPage().getPageId().getContainerId(), bcursor.getP().getPage().getPageId().getPageNumber());
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
         * Repairs underflow when an about-to-underflow child is encountered during update
         * mode traversal. Both the parent page (bcursor.p) and its child page (bcursor.q) must
         * be latched in UPDATE mode prior to calling this method. When this method returns,
         * the latch on the parent page will have been released, and the child page that covers the
         * search key will remain latched in bcursor.q. 
         * <p>
         * For this algorithm to work, an index page needs to have at least two children who
         * are linked the index page.
         */
        public final boolean doRepairPageUnderflow(Transaction trx,
                BTreeCursor bcursor) {

        	Trace.event(50);
            assert bcursor.getP() != null;
            assert bcursor.getP().isLatchedForUpdate();

            assert bcursor.getQ() != null;
            assert bcursor.getQ().isLatchedForUpdate();

            BTreeNode q = getBTreeNode();
            q.wrap((SlottedPage) bcursor.getQ().getPage());

            BTreeNode p = getBTreeNode();
            p.wrap((SlottedPage) bcursor.getP().getPage());

            BTreeNode r = getBTreeNode();

            int Q = q.page.getPageId().getPageNumber();

            IndexItem u = q.getHighKey();
            IndexItem highkeyP = p.getHighKey();
            /*
             * If the high key of Q is less than the high key of
             * P then Q is not the rightmost child of P.
             */
            if (u.compareTo(highkeyP) < 0) {
            	Trace.event(51);
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
                assert v.getChildPageNumber() == q.getPage().getPageId().getPageNumber();
                
                /* R = rightsibling of Q */
                int R = q.header.rightSibling;
                assert R != -1;

                PageId rightPageId = new PageId(containerId,R);
                Trace.event(52, rightPageId.getContainerId(), rightPageId.getPageNumber());
                bcursor.setR(btreeMgr.bufmgr.fixForUpdate(rightPageId, 0));
                r.wrap((SlottedPage) bcursor.getR().getPage());
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
                	Trace.event(53);
                    bcursor.unfixP();
                    if (q.canMergeWith(r)) {
                    	Trace.event(54);
                        doMerge(trx, bcursor);
                    } else {
                        // FIXME TEST case
                        doRedistribute(trx, bcursor);
                    }
                } else {
                	Trace.event(55);
                    /* b) or c) R is a direct child of P */
                    /* w = index record associated with R in P */
                    IndexItem w = p.findIndexItem(R);
                    assert w != null;
                    assert w.getChildPageNumber() == r.getPage().getPageId().getPageNumber();
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
                        Trace.event(56);
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
                        	Trace.event(57);
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
                            p.wrap((SlottedPage) bcursor.getP().getPage());
                            if (Validating) {
                            	assert p.findIndexItem(Q) != null;
                            }
                            assert bcursor.getQ().getPage().getPageId().getPageNumber() == Q;
                            
                            if (p.findIndexItem(R) == null) {
                            	Trace.event(58);
                                /* R is not a child of P anymore.
                                 * We need to restart the algorithm
                                 */
                                // FIXME TEST case
                                bcursor.unfixR();
                                return true;
                            }
                            assert bcursor.getR().getPage().getPageId().getPageNumber() == R;
                        }
                        /*
                         * We need to link S to P. Since our cursor is currently
                         * positioned Q, we need to temporarily move right to R,
                         * in order to do the link.
                         */
                        BufferAccessBlock savedQ = bcursor.removeQ(); // Save Q
                        bcursor.setQ(bcursor.removeR()); // Now Q is R
                        try {
                        	Trace.event(59);
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
                        p.wrap((SlottedPage) bcursor.getP().getPage());
                        assert p.findIndexItem(S) != null;
                    }

                    p.wrap((SlottedPage) bcursor.getP().getPage());
                    assert p.findIndexItem(Q) != null;
                    assert p.findIndexItem(R) != null;
                    
                    /*
                     * At this point any sibling of R (ie, S) is
                     * guaranteed to be linked to parent P. So we can now
                     * unlink R from P to allow merging of Q and R.
                     */
                    Trace.event(60);
                    doUnlink(trx, bcursor); // Now we can unlink R from P
                    /*
                     * Merge Q and R
                     */
                    q.wrap((SlottedPage) bcursor.getQ().getPage());
                    r.wrap((SlottedPage) bcursor.getR().getPage());
                    if (q.canMergeWith(r)) {
                    	Trace.event(61);
                        doMerge(trx, bcursor);
                    } else {
                        // FIXME TEST case
                        doRedistribute(trx, bcursor);
                    }
                }
            } else {
            	Trace.event(62);
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
                PageId l_pageId = new PageId(containerId,L);
                Trace.event(63, l_pageId.getContainerId(), l_pageId.getPageNumber());
                bcursor.setQ(btreeMgr.bufmgr.fixForUpdate(l_pageId, 0));
                q.wrap((SlottedPage) bcursor.getQ().getPage());
                /* The node to the right of L is N */
                /* This may or may not be Q depending upon whether N is
                 * an indirect child of P.
                 */
                int N = q.header.rightSibling;
                assert N != -1;
                PageId n_pageId = new PageId(containerId, N);
                Trace.event(64, n_pageId.getContainerId(), n_pageId.getPageNumber());
                bcursor.setR(btreeMgr.bufmgr.fixForUpdate(n_pageId, 0));
                r.wrap((SlottedPage) bcursor.getR().getPage());

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
                	Trace.event(65);
                    assert q.header.rightSibling == Q;
                    assert N == Q;
                    /*
                     * remember that Q holds L and R holds N (N == original Q). 
                     */
                    if (!r.isAboutToUnderflow()) {
                    	Trace.event(66);
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
                    	Trace.event(67);
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
                        Trace.event(68);
                        q.wrap((SlottedPage) bcursor.getQ().getPage());
                        r.wrap((SlottedPage) bcursor.getR().getPage());
                        
                        if (q.canMergeWith(r)) {
                        	/* merge L and Q */
                        	Trace.event(69);
                            doMerge(trx, bcursor);
                        } else {
                            // FIXME Test case
                            doRedistribute(trx, bcursor);
                        }
                        /* Q = L (already true) */
                        assert bcursor.getQ().getPage().getPageId().getPageNumber() == L;
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
                	Trace.event(70);
                    if (!p.canAccomodate(u)) {
                    	Trace.event(71);
                        doSplitParent(trx, bcursor);
                        /*
                         * Since Q was the rightmost child of P,
                         * even after the split Q and its left sibling L must 
                         * belong to P (because a minimum of 2 items must be
                         * present in a page).
                         */
                        p.wrap((SlottedPage) bcursor.getP().getPage());
                        assert p.findIndexItem(Q) != null;
                        assert p.findIndexItem(L) != null;
                    }
                    /* link N to parent P */
                    Trace.event(72);
                    doLink(trx, bcursor);
                    /* unlatch L */
                    bcursor.unfixQ();
                    /* N becomes bcursor.q */
                    bcursor.setQ(bcursor.removeR());
                    q.wrap((SlottedPage) bcursor.getQ().getPage());
                    /* latch Q again, which now becomes bcursor.r */
                    assert Q == q.header.rightSibling;
                    PageId q_pageId = new PageId(containerId, Q);
                    Trace.event(73, q_pageId.getContainerId(), q_pageId.getPageNumber());
                    bcursor.setR(btreeMgr.bufmgr.fixForUpdate(q_pageId, 0));
                    r.wrap((SlottedPage) bcursor.getR().getPage());
                    if (!r.isAboutToUnderflow()) {
                        /* Q is no longer about to underflow */
                        // FIXME Test case
                    	Trace.event(74);
                        bcursor.unfixP();
                        bcursor.unfixQ();
                        bcursor.setQ(bcursor.removeR());
                        
                    } else {
                        /* unlink Q from parent P (note that bcursor.r points to Q) */
                    	Trace.event(75);
                        doUnlink(trx, bcursor);
                        /* here bcursor.q is N and bcursor.r is Q */
                        q.wrap((SlottedPage) bcursor.getQ().getPage());
                        r.wrap((SlottedPage) bcursor.getR().getPage());
                        if (q.canMergeWith(r)) {
                            /* merge N and Q */
                        	Trace.event(76);
                            doMerge(trx, bcursor);
                        } else {
                            doRedistribute(trx, bcursor);
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
         * Repairs underflow when an about-to-underflow child is encountered during update
         * mode traversal. Both the parent page (bcursor.p) and its child page (bcursor.q) must
         * be latched in UPDATE mode prior to calling this method. When this method returns,
         * the latch on the parent page will have been released, and the child page that covers the
         * search key will remain latched in bcursor.q. 
         * <p>
         * For this algorithm to work, an index page needs to have at least two children who
         * are linked the index page.
         */
        public final void repairPageUnderflow(Transaction trx,
                BTreeCursor bcursor) {
            boolean tryAgain = doRepairPageUnderflow(trx, bcursor);
            while (tryAgain) {
                // FIXME Test case
            	Trace.event(77);
                tryAgain = doRepairPageUnderflow(trx, bcursor);
            }
        }

        /**
         * Walks down the tree using UPDATE mode latching. On the way down, pages may be
         * split or merged to ensure that the tree maintains its balance. 
         * bcursor.searchKey represents the key being searched for.
         * When this returns bcursor.p will point to the page that contains or should
         * contain the search key. This page will be latched in UPDATE mode.
         * <p>This traversal mode is used for inserts and deletes.
         * Corresponds to Update-mode-traverse in btree paper.
         */
        public final void updateModeTraverse(Transaction trx, BTreeCursor bcursor) {
            /*
             * Fix root page
             */
        	PageId rootPageId = new PageId(
                    containerId,
                    BTreeIndexManagerImpl.ROOT_PAGE_NUMBER);
        	Trace.event(78, rootPageId.getContainerId(), rootPageId.getPageNumber());
            bcursor.setP(btreeMgr.bufmgr.fixForUpdate(rootPageId, 0));
            BTreeNode p = getBTreeNode();
            p.wrap((SlottedPage) bcursor.getP().getPage());
            if (p.header.rightSibling != -1) {
                /* 
                 * Root page has a right sibling. This means that the root page
                 * was split at some point, but the parent has not yet been
                 * created.
                 * A new child page will be allocated and the root will become
                 * the parent of this new child, and its right sibling. 
                 */
            	PageId r_pageId = new PageId(
                        containerId,
                        p.header.rightSibling);
                bcursor.setQ(bcursor.removeP());
                Trace.event(79, r_pageId.getContainerId(), r_pageId.getPageNumber());
                bcursor.setR(btreeMgr.bufmgr.fixForUpdate(r_pageId, 0));
                doIncreaseTreeHeight(trx, bcursor);
                bcursor.setP(bcursor.removeQ());
                p.wrap((SlottedPage) bcursor.getP().getPage());
            }
            if (p.isLeaf()) {
                return;
            }
            int childPageNumber = p.findChildPage(bcursor.searchKey);
            PageId q_pageId = new PageId(
                    containerId,
                    childPageNumber);
            Trace.event(80, q_pageId.getContainerId(), q_pageId.getPageNumber());
            bcursor.setQ(btreeMgr.bufmgr.fixForUpdate(q_pageId, 0));
            BTreeNode q = getBTreeNode();
            q.wrap((SlottedPage) bcursor.getQ().getPage());
            boolean childPageLatched = true;
            if (p.isRoot() && p.header.keyCount == 1
                    && q.header.rightSibling == -1) {
                /* Q is only child of P and Q has no right sibling */
                /*
                 * Root is underflown as it has only one child and this child does not
                 * have a right sibling. Decrease the height of the tree by
                 * merging the child page into the root.
                 */
            	Trace.event(81);
                doDecreaseTreeHeight(trx, bcursor);
                childPageLatched = false;
            }
            p.wrap((SlottedPage) bcursor.getP().getPage());
            while (!p.isLeaf()) {
                if (!childPageLatched) {
                    /*
                     * BUG in published algorithm - need to avoid latching
                     * Q if already latched.
                     */
                    childPageNumber = p.findChildPage(bcursor.searchKey);
                    q_pageId = new PageId(
                            containerId,
                            childPageNumber);
                    Trace.event(82, q_pageId.getContainerId(), q_pageId.getPageNumber());
                    bcursor.setQ(btreeMgr.bufmgr.fixForUpdate(q_pageId, 0));
                    q.wrap((SlottedPage) bcursor.getQ().getPage());
                    /*
                     * No need to set childPageLatch as this is only needed first time round.
                     */
                } else {
                    childPageLatched = false;
                }
                if (q.isAboutToUnderflow()) {
                	Trace.event(83, q.getPage().getPageId().getContainerId(), q.getPage().getPageId().getPageNumber());
                    repairPageUnderflow(trx, bcursor);
                    bcursor.setP(bcursor.removeQ());
                } else {
                    IndexItem v = p.findIndexItem(q.page
                        .getPageId()
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
                    	Trace.event(84);
                        if (!p.canAccomodate(u)) {
                        	Trace.event(85);
                            doSplitParent(trx, bcursor);
                        }
                        Trace.event(86);
                       	doLink(trx, bcursor);
                    }
                    assert bcursor.getP().isLatchedForUpdate();
                    assert bcursor.getQ().isLatchedForUpdate();
                    q.wrap((SlottedPage) bcursor.getQ().getPage());
                    if (q.getHighKey().compareTo(bcursor.searchKey) >= 0) {
                        /* Q covers search key */
                    	Trace.event(87);
                        bcursor.unfixP();
                        bcursor.setP(bcursor.removeQ());
                    } else {
                        /* move right */
                        int rightsibling = q.header.rightSibling;
                        bcursor.unfixP();
                        assert q.header.rightSibling != -1;
                        PageId r_pageId = new PageId(
                                containerId,
                                rightsibling);
                        Trace.event(88, r_pageId.getContainerId(), r_pageId.getPageNumber());
                        bcursor.setP(btreeMgr.bufmgr.fixForUpdate(r_pageId, 0));
                        bcursor.unfixQ();
                    }
                }
                p.wrap((SlottedPage) bcursor.getP().getPage());
            }
        }

        /**
         * Walks down the tree acquiring shared latches on the way.
         * bcursor.searchKey represents the key being searched for.
         * When this returns bcursor.p will point to the leaf page that should
         * contain the search key. bcursor.p will be left in shared latch.
         * Corresponds to read-mode-traverse() in btree paper. 
         */
        public final void readModeTraverse(BTreeCursor bcursor) {

            if (log.isDebugEnabled()) {
                log.debug(
                    LOG_CLASS_NAME,
                    "readModeTraverse",
                    "SIMPLEDBM-DEBUG: Read mode traverse for search key = "
                            + bcursor.getSearchKey());
            }

            /*
             * Fix root page
             */
            PageId rootPageId = new PageId(
                    containerId,
                    BTreeIndexManagerImpl.ROOT_PAGE_NUMBER);
            Trace.event(89, rootPageId.getContainerId(), rootPageId.getPageNumber());
            bcursor.setP(btreeMgr.bufmgr.fixShared(rootPageId, 0));
            BTreeNode p = getBTreeNode();
            p.wrap((SlottedPage) bcursor.getP().getPage());

            do {
                IndexItem v = p.getHighKey();
                if (v.compareTo(bcursor.getSearchKey()) < 0) {
                    // Move right as the search key is greater than the highkey
                    // FIXME TEST case
                    int rightsibling = p.header.rightSibling;
                    PageId r_pageId = new PageId(
                            containerId,
                            rightsibling);
                    Trace.event(90, r_pageId.getContainerId(), r_pageId.getPageNumber());
                    bcursor.setQ(btreeMgr.bufmgr.fixShared(r_pageId, 0));
                    bcursor.unfixP();
                    bcursor.setP(bcursor.removeQ());
                    p.wrap((SlottedPage) bcursor.getP().getPage());
                    assert p.getHighKey().compareTo(bcursor.getSearchKey()) >= 0;
                }
                if (!p.isLeaf()) {
                    // find the child page and move down
                    int childPageNumber = p.findChildPage(bcursor.searchKey);
                    PageId c_pageId = new PageId(
                            containerId,
                            childPageNumber);
                    Trace.event(91, c_pageId.getContainerId(), c_pageId.getPageNumber());
                    bcursor.setQ(btreeMgr.bufmgr.fixShared(c_pageId, 0));
                    bcursor.unfixP();
                    bcursor.setP(bcursor.removeQ());
                    p.wrap((SlottedPage) bcursor.getP().getPage());
                }
            } while (!p.isLeaf());
        }

        /**
         * Traverses a BTree down to the leaf level, and prepares the leaf page
         * for inserting the new key. bcursor.p must hold the root node
         * in update mode latch when this is called. When this returns
         * bcursor.p will point to the page where the insert should take place,
         * and will be exclusively latched.
         * @return SearchResult containing information about where to insert the new key
         */
        public final SearchResult doInsertTraverse(Transaction trx,
                BTreeCursor bcursor) {
            updateModeTraverse(trx, bcursor);
            /* At this point p points to the leaf page where the key is to be inserted */
            assert bcursor.getP() != null;
            assert bcursor.getP().isLatchedForUpdate();
            BTreeNode node = getBTreeNode();
            node.wrap((SlottedPage) bcursor.getP().getPage());
            assert node.isLeaf();
            assert !node.isDeallocated();
            assert node.getHighKey().compareTo(bcursor.searchKey) >= 0;
            if (!node.canAccomodate(bcursor.searchKey)) {
            	Trace.event(92, bcursor.getP().getPage().getPageId().getContainerId(), bcursor.getP().getPage().getPageId().getPageNumber());
                bcursor.setQ(bcursor.removeP());
                doSplit(trx, bcursor);
                bcursor.setP(bcursor.removeQ());
            }
            Trace.event(93, bcursor.getP().getPage().getPageId().getContainerId(), bcursor.getP().getPage().getPageId().getPageNumber());
            bcursor.getP().upgradeUpdateLatch();
            node.wrap((SlottedPage) bcursor.getP().getPage());
            SearchResult sr = node.search(bcursor.searchKey);
            return sr;
        }

        /**
         * Obtain a lock on the next key. Mode and duration are specified by the caller.
         * bcursor.P must contain the current page, latched exclusively. If the
         * lock on next key is successful, the page will be left latched. Else, the latch on
         * the page will be released.
         * 
         * @return True if insert can proceed, false if lock could not be obtained on next key.
         */
        public final boolean doNextKeyLock(Transaction trx,
                BTreeCursor bcursor, int nextPageNumber, int nextk,
                LockMode mode, LockDuration duration) {
        	
        	assert bcursor.getP() != null;
        	assert bcursor.getP().isLatchedExclusively();
        	
            SlottedPage nextPage = null;
            IndexItem nextItem = null;
            Lsn nextPageLsn = null;
            // Make a note of current page and page Lsn
            int currentPageNumber = bcursor
                .getP()
                .getPage()
                .getPageId()
                .getPageNumber();
            Lsn currentPageLsn = bcursor.getP().getPage().getPageLsn();
            bcursor.setNextKeyLocation(null);
            if (nextPageNumber != -1) {
                // next key is in the right sibling page
            	PageId r_pageId = new PageId(
                        containerId,
                        nextPageNumber);
            	Trace.event(94, r_pageId.getContainerId(), r_pageId.getPageNumber());
                bcursor.setR(btreeMgr.bufmgr.fixShared(r_pageId, 0));
                nextPage = (SlottedPage) bcursor.getR().getPage();
                BTreeNode nextNode = getBTreeNode();
                nextNode.wrap(nextPage);
                nextItem = nextNode.getItem(nextk);
                nextPageLsn = nextPage.getPageLsn();
            } else {
                // next key is in the current page
                BTreeNode nextNode = getBTreeNode();
                nextNode.wrap((SlottedPage) bcursor.getP().getPage());
                nextPageLsn = bcursor.getP().getPage().getPageLsn();
                if (nextk == -1) {
                    nextItem = nextNode.getHighKey(); // represents infinity
                } else {
                    nextItem = nextNode.getItem(nextk);
                }
            }
            try {
            	// TODO: add nextItem.getLocation() to trace message
            	Trace.event(95, nextItem.getLocation().getContainerId(), nextItem.getLocation().getX(), nextItem.getLocation().getY());
            	trx.acquireLockNowait(nextItem.getLocation(), mode, duration);
                bcursor.setNextKeyLocation(nextItem.getLocation());
                /*
                 * Instant duration lock succeeded. We can proceed with the insert.
                 */
                return true;
            } catch (LockException e) {

                if (log.isDebugEnabled()) {
                    log.debug(
                        LOG_CLASS_NAME,
                        "doNextKeyLock",
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
            	Trace.event(96, nextItem.getLocation().getContainerId(), nextItem.getLocation().getX(), nextItem.getLocation().getY());
                trx.acquireLock(nextItem.getLocation(), mode, duration);
                bcursor.setNextKeyLocation(nextItem.getLocation());
                /*
                 * Now we have obtained the lock.
                 * We can continue from where we were if nothing has changed in the meantime
                 */
                PageId c_pageId = new PageId(
                        containerId,
                        currentPageNumber);
                Trace.event(97, c_pageId.getContainerId(), c_pageId.getPageNumber());
                bcursor.setP(btreeMgr.bufmgr.fixExclusive(c_pageId, false, -1, 0));
                if (nextPageNumber != -1) {
                	PageId n_pageId = new PageId(
                            containerId,
                            nextPageNumber);
                	Trace.event(98, n_pageId.getContainerId(), n_pageId.getPageNumber());
                    bcursor.setR(btreeMgr.bufmgr.fixShared(n_pageId, 0));
                }
                if (currentPageLsn.compareTo(bcursor
                    .getP()
                    .getPage()
                    .getPageLsn()) == 0) {
                    if (nextPageNumber == -1
                            || nextPageLsn.compareTo(bcursor
                                .getR()
                                .getPage()
                                .getPageLsn()) == 0) {
                    	Trace.event(99);
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
            Trace.event(100);
            Trace.event(101, bcursor.getNextKeyLocation().getContainerId(), bcursor.getNextKeyLocation().getX(), bcursor.getNextKeyLocation().getY());
            trx.releaseLock(bcursor.getNextKeyLocation());
            bcursor.setNextKeyLocation(null);
            return false;
        }

        /**
         * @see #insert(Transaction, IndexKey, Location) 
         */
        public final boolean doInsert(Transaction trx, IndexKey key,
                Location location) {

            BTreeCursor bcursor = new BTreeCursor(btreeMgr);
            bcursor.searchKey = new IndexItem(
                key,
                location,
                -1,
                true,
                isUnique());

            try {
                /*
                 * Traverse to leaf page
                 */
                SearchResult sr = doInsertTraverse(trx, bcursor);

                int nextKeyPage = -1;
                int nextk = -1;

                if (sr.k == -1) {
                    /* next key is in the next page or it is
                     * INFINITY if this is the rightmost page.
                     */
                    BTreeNode node = getBTreeNode();
                    node.wrap((SlottedPage) bcursor.getP().getPage());
                    nextKeyPage = node.header.rightSibling;
                    if (nextKeyPage != -1) {
                        nextk = FIRST_KEY_POS; // first key of next page
                        Trace.event(102);
                    }
                    else {
                    	Trace.event(103);
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
                            	Trace.event(104, loc.getContainerId(), loc.getX(), loc.getY());
                                trx.acquireLockNowait(
                                    loc,
                                    LockMode.SHARED,
                                    duration);
                            } catch (LockException e) {
                                // FIXME Test case
                                if (log.isDebugEnabled()) {
                                    log.debug(
                                        LOG_CLASS_NAME,
                                        "doInsert",
                                        "SIMPLEDBM-DEBUG: Failed to acquire NOWAIT "
                                                + LockMode.SHARED + " lock on "
                                                + loc);
                                }
                                /*
                                 * Failed to acquire conditional lock. 
                                 * Need to unlatch page and retry unconditionally. 
                                 */
                                bcursor.unfixP();
                                Trace.event(105, loc.getContainerId(), loc.getX(), loc.getY());
                                trx.acquireLock(loc, LockMode.SHARED, duration);
                                /*
                                 * We have obtained the lock. We need to double check that the key
                                 * still exists.
                                 */
                                PageId rootPageId = new PageId(
                                        containerId,
                                        BTreeIndexManagerImpl.ROOT_PAGE_NUMBER);
                                Trace.event(106);
                                Trace.event(107, rootPageId.getContainerId(), rootPageId.getPageNumber());
                                bcursor
                                    .setP(btreeMgr.bufmgr
                                        .fixForUpdate(rootPageId,0));
                                sr = doInsertTraverse(trx, bcursor);
                            }
                            if (sr.exactMatch
                                    && sr.item.getLocation().equals(loc)) {
                            	Trace.event(108);
                                /*
                                 * Mohan says that for RR we need a commit duration lock, but
                                 * for CS, maybe we should release the lock here??
                                 */
                                if (trx.getIsolationMode() == IsolationMode.CURSOR_STABILITY
                                        || trx.getIsolationMode() == IsolationMode.READ_COMMITTED) {
                                	Trace.event(109, loc.getContainerId(), loc.getX(), loc.getY());
                                    trx.releaseLock(loc);
                                }
                                log.warn(LOG_CLASS_NAME, "doInsert", mcat
                                    .getMessage("WB0003", key, location));
                                throw new UniqueConstraintViolationException(
                                    mcat.getMessage("WB0003", key, location));
                            }
                            /*
                             * Key has been deleted or has been rolled back in the meantime
                             */
                            Trace.event(110);
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
                if (!doNextKeyLock(
                    trx,
                    bcursor,
                    nextKeyPage,
                    nextk,
                    LockMode.EXCLUSIVE,
                    LockDuration.MANUAL_DURATION)) {
                    return false;
                }
                /*
                 * We can finally insert the key and be done with!
                 */
                InsertOperation insertOp = (InsertOperation) btreeMgr.loggableFactory
                    .getInstance(
                        BTreeIndexManagerImpl.MODULE_ID,
                        BTreeIndexManagerImpl.TYPE_INSERT_OPERATION);
                insertOp.setKeyFactoryType(keyFactoryType);
                insertOp.setLocationFactoryType(locationFactoryType);
                insertOp.setItem(bcursor.searchKey);
                insertOp.setUnique(isUnique());

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
                Trace.event(111, bcursor.getNextKeyLocation().getContainerId(), bcursor.getNextKeyLocation().getX(), bcursor.getNextKeyLocation().getY());
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
        	Trace.event(112);
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

            BTreeCursor bcursor = new BTreeCursor(btreeMgr);
            bcursor.searchKey = new IndexItem(
                key,
                location,
                -1,
                true,
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
                BTreeNode node = getBTreeNode();
                node.wrap((SlottedPage) bcursor.getP().getPage());
                assert node.isLeaf();
                assert !node.isDeallocated();
                assert node.getHighKey().compareTo(bcursor.searchKey) >= 0;

                SearchResult sr = node.search(bcursor.searchKey);
                if (!sr.exactMatch) {
                	Trace.dump();
                    // key not found?? something is wrong
                    log.error(LOG_CLASS_NAME, "doDelete", mcat.getMessage(
                        "EB0004",
                        bcursor.searchKey.toString() + " TID " + Thread.currentThread().getId()));
                    //node.dumpAsXml();
                    throw new IndexException(mcat.getMessage(
                        "EB0004",
                        bcursor.searchKey.toString()));
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
                if (!doNextKeyLock(
                    trx,
                    bcursor,
                    nextKeyPage,
                    nextk,
                    LockMode.EXCLUSIVE,
                    LockDuration.COMMIT_DURATION)) {
                    return false;
                }
                /*
                 * Now we can delete the key and be done with!
                 */
                DeleteOperation deleteOp = (DeleteOperation) btreeMgr.loggableFactory
                    .getInstance(
                        BTreeIndexManagerImpl.MODULE_ID,
                        BTreeIndexManagerImpl.TYPE_DELETE_OPERATION);
                deleteOp.setKeyFactoryType(keyFactoryType);
                deleteOp.setLocationFactoryType(locationFactoryType);
                deleteOp.setItem(bcursor.searchKey);
                deleteOp.setUnique(isUnique());

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
         * Delete specified key and location combination. It is an error if the key is not found.
         * It is assumed that location is already locked in exclusive mode. At the end of this
         * operation, the next key will be locked in exclusive mode, and the lock on location may be
         * released.
         */
        public final void delete(Transaction trx, IndexKey key,
                Location location) {
        	Trace.event(113);
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
         */
        final SearchResult doSearch(IndexCursorImpl icursor) {
            BTreeNode node = getBTreeNode();
            node.wrap((SlottedPage) icursor.bcursor.getP().getPage());
            // node.dump();
            icursor.setEof(false);
            SearchResult sr = null;
            if (icursor.scanMode == IndexCursorImpl.SCAN_FETCH_GREATER
                    && node.getPage().getPageLsn().equals(icursor.pageLsn)) {
                // If this is a call to fetchNext, check if we can avoid searching the node
                // If page LSN hasn't changed we can used cached values.
                sr = new SearchResult();
                sr.item = icursor.currentKey;
                sr.k = icursor.k;
                sr.exactMatch = true;
                if (!node.getItem(sr.k).equals(icursor.currentKey)) {
                    log.error(LOG_CLASS_NAME, "redoLinkOperation", mcat
                        .getMessage(
                            "EB0005",
                            node.getItem(sr.k),
                            icursor.currentKey));
                    throw new IndexException(mcat.getMessage("EB0005", node
                        .getItem(sr.k), icursor.currentKey));
                }
            } else {
                sr = node.search(icursor.currentKey);
            }

            // if (sr.exactMatch && icursor.fetchCount > 0) {
            if (sr.exactMatch
                    && icursor.scanMode == IndexCursorImpl.SCAN_FETCH_GREATER) {
                /*
                 * If we have an exact match, there are two possibilities.
                 * If this is the first call to fetch (fetchCount == 0), then
                 * we are done. Else, we need to move to the next key.
                 */
                if (sr.k == node.getKeyCount()) {
                    /*
                     * Current key is the last key on this node. Therefore,
                     * move to the node to the right.
                     */
                    return moveToNextNode(icursor, node, sr);
                } else {
                    /*
                     * Move to the next key in the same node
                     */
                    sr.k = sr.k + 1;
                    sr.exactMatch = false;
                    sr.item = node.getItem(sr.k);
                }
            } else if (sr.k == -1) {
                /*
                 * The current key is greater than all keys in this node,
                 * therefore we need to move to the node to our right.
                 */
                return moveToNextNode(icursor, node, sr);
            } else if (node.header.keyCount == 1) {
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
         * Moves to the node to the right if possible or sets EOF status.
         * Leaves current page latched in SHARED mode.
         */
        private SearchResult moveToNextNode(IndexCursorImpl icursor,
                BTreeNode node, SearchResult sr) {
            // Key to be fetched is in the next page
            int nextPage = node.header.rightSibling;
            if (nextPage == -1) {
                /*
                 * There isn't a node to our right, so we are at EOF.
                 */
                icursor.setEof(true);
                sr.k = node.header.keyCount;
                sr.exactMatch = false;
                sr.item = node.getItem(sr.k);
            } else {
                PageId nextPageId = new PageId(containerId, nextPage);
                icursor.bcursor.setQ(icursor.bcursor.removeP());
                icursor.bcursor.setP(btreeMgr.bufmgr.fixShared(nextPageId, 0));
                node.wrap((SlottedPage) icursor.bcursor.getP().getPage());
                icursor.bcursor.unfixQ();
                sr = node.search(icursor.currentKey);
            }
            return sr;
        }

        /**
         * Fetches the next available key, after locking the corresponding Location in the
         * specified mode. Handles the situation where the current key has been deleted.
         * 
         * @param trx Transaction that is managing this fetch
         * @param cursor The BTreeScan cursor
         * @return True if successful, fals if operation needs to be tried again 
         */
        private final boolean doFetch(Transaction trx, IndexScan cursor) {
            IndexCursorImpl icursor = (IndexCursorImpl) cursor;
            try {
                boolean doSearch = false;
                BTreeNode node = getBTreeNode();
                // if (icursor.fetchCount > 0) {
                if (icursor.scanMode == IndexCursorImpl.SCAN_FETCH_GREATER) {
                    // This is not the first call to fetch
                    // Check to see if the BTree should be scanned to locate the key
                    icursor.bcursor.setP(btreeMgr.bufmgr.fixShared(
                        icursor.pageId,
                        0));
                    node.wrap((SlottedPage) icursor.bcursor.getP().getPage());
                    if (node.isDeallocated() || !node.isLeaf()) {
                        // The node that contained current key is no longer part of the tree, hence scan is necessary
                        doSearch = true;
                        icursor.bcursor.unfixP();
                    } else {
                        // The node still exists, so we need to check whether the previously returned key is bound to the 
                        // node.
                        if (!node
                            .getPage()
                            .getPageLsn()
                            .equals(icursor.pageLsn)
                                && !node.covers(icursor.currentKey)) {
                            // The previous key is no longer bound to the node
                            doSearch = true;
                            icursor.bcursor.unfixP();
                        }
                    }
                } else {
                    // First call to fetch, hence tree must be scanned.
                    doSearch = true;
                }

                if (doSearch) {
                    icursor.bcursor.setSearchKey(icursor.currentKey);
                    readModeTraverse(icursor.bcursor);
                }

                SearchResult sr = doSearch(icursor);

                Savepoint sp = trx.createSavepoint(false);
                try {
                    if (sr.item == null) {
                        log.error(LOG_CLASS_NAME, "doFetch", mcat.getMessage(
                            "EB0006",
                            icursor.currentKey.toString()));
                        throw new IndexException(mcat.getMessage(
                            "EB0006",
                            icursor.currentKey.toString()));
                    }
                    trx.acquireLockNowait(
                        sr.item.getLocation(),
                        icursor.lockMode,
                        LockDuration.MANUAL_DURATION);
                    icursor.currentKey = sr.item;
                    icursor.pageId = icursor.bcursor
                        .getP()
                        .getPage()
                        .getPageId();
                    icursor.pageLsn = icursor.bcursor
                        .getP()
                        .getPage()
                        .getPageLsn();
                    icursor.k = sr.k;
                    icursor.bcursor.unfixP();
                    icursor.fetchCount++;
                    icursor.scanMode = IndexCursorImpl.SCAN_FETCH_GREATER;
                    
                    /*
                     * FIXME - If isolationMode is READ_COMMITTED and the current
                     * key does not match search criteria (NOT FOUND), then we 
                     * should release the lock on the key here.
                     * At present this is handled by closing the scan.
                     */
                    return true;
                } catch (LockException e) {

                    if (log.isDebugEnabled()) {
                        log.debug(
                            LOG_CLASS_NAME,
                            "doFetch",
                            "SIMPLEDBM-DEBUG: Failed to acquire conditional "
                                    + icursor.lockMode + " lock on "
                                    + sr.item.getLocation());
                    }
                    /*
                     * Failed to acquire conditional lock. 
                     * Need to unlatch page and retry unconditionally. 
                     */
                    icursor.bcursor.unfixP();
                    // System.out.println(Thread.currentThread().getName() + ":doFetch: Conditional lock failed, attempt to acquire unconditional lock on " + sr.item.getLocation() + " in mode " + icursor.lockMode);
                    trx.acquireLock(
                        sr.item.getLocation(),
                        icursor.lockMode,
                        LockDuration.MANUAL_DURATION);
                    /*
                     * We have obtained the lock. We need to double check that the searched key
                     * still exists.
                     */
                    // TODO - could avoid the tree traverse here by checking the old page
                    icursor.bcursor.setSearchKey(icursor.currentKey);
                    readModeTraverse(icursor.bcursor);
                    SearchResult sr1 = doSearch(icursor);
                    if (sr1.item.equals(sr.item)) {
                        // we found the same key again
                        icursor.currentKey = sr1.item;
                        icursor.pageId = icursor.bcursor
                            .getP()
                            .getPage()
                            .getPageId();
                        icursor.pageLsn = icursor.bcursor
                            .getP()
                            .getPage()
                            .getPageLsn();
                        icursor.k = sr.k;
                        icursor.bcursor.unfixP();
                        icursor.fetchCount++;
                        icursor.scanMode = IndexCursorImpl.SCAN_FETCH_GREATER;
                        /*
                         * FIXME - If isolationMode is READ_COMMITTED and the current
                         * key does not match search criteria (NOT FOUND), then we 
                         * should release the lock on the key here.
                         * At present this is handled by closing the scan.
                         */
                        return true;
                    }
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

        public final void fetch(Transaction trx, IndexScan cursor) {
            boolean success = doFetch(trx, cursor);
            while (!success) {
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
            IndexCursorImpl icursor = new IndexCursorImpl(
                trx,
                this,
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
         * Used by fetch routines. Initially set to the user supplied search key. As the scan
         * moves, tracks the current key the cursor is positioned on.
         * The associated location must always be locked.
         */
        IndexItem currentKey;

        /**
         * Saved reference to previous key - used to release
         * lock on previous key in certain Isolation modes.
         */
        IndexItem previousKey;

        /**
         * Cached value of {@link SearchResult#k}. If the last page we 
         * were at hasn't changed, then we can use the cached value to avoid
         * searching the page.
         */
        int k = 0;

        /**
         * Keeps a count of the number of keys fetched so far.
         */
        int fetchCount = 0;

        /**
         * Internal use
         */
        final BTreeCursor bcursor;

        /**
         * The lock mode to be used for locking locations retrieved by the scan.
         */
        LockMode lockMode;

        /**
         * The BTree that we are scanning.
         */
        final BTreeImpl btree;

        /**
         * Indicates whether we have reached end of file. Also set when {@link #fetchCompleted(boolean)}
         * is invoked with an argument of false.
         */
        private boolean eof = false;

        /**
         * Transaction that is managing this scan. 
         */
        final Transaction trx;

        /**
         * Initially set to {@link #SCAN_FETCH_GREATER_OR_EQUAL}, and after the first
         * fetch, set to {@link #SCAN_FETCH_GREATER}. The scan mode determines how the fetch
         * will operate.
         */
        int scanMode = 0;

        int stateFetchCompleted = 0;

        static final int SCAN_FETCH_GREATER_OR_EQUAL = 1;

        static final int SCAN_FETCH_GREATER = 2;

        IndexCursorImpl(Transaction trx, BTreeImpl btree, IndexItem startKey,
                LockMode lockMode) {
            this.btree = btree;
            this.bcursor = new BTreeCursor(btree.btreeMgr);
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
                log.warn(
                    LOG_CLASS_NAME,
                    "close",
                    "fetchCompleted() has not been called after fetchNext()");
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
                            if (log.isDebugEnabled()) {
                                log.debug(
                                    LOG_CLASS_NAME,
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
                            if (log.isDebugEnabled()) {
                                log.debug(
                                    LOG_CLASS_NAME,
                                    IndexCursorImpl.class.getName()
                                            + ".fetchNext",
                                    "SIMPLEDBM-DEBUG: Downgrading lock on previous row "
                                            + previousKey.getLocation()
                                            + " to " + LockMode.SHARED);
                            }
                            trx.downgradeLock(
                                previousKey.getLocation(),
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

        public final void fetchCompleted(boolean matched) {
            /*
             * This method is invoked after the data from tuple container has been
             * read. Its main purpose is to release locks in read committed or cursor stability mode.
             */
            stateFetchCompleted = 0;
            if (!matched && !isEof()) {
                setEof(true);
            }
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
                        trx.downgradeLock(
                            currentKey.getLocation(),
                            LockMode.SHARED);
                    }
                }
            }
        }

        public final void close() {
            trx.unregisterTransactionalCursor(this);
            RSSException ex = null;

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
                            if (log.isDebugEnabled()) {
                                log
                                    .debug(
                                        LOG_CLASS_NAME,
                                        this.getClass().getName() + ".close",
                                        "SIMPLEDBM-DEBUG: Releasing lock on current row "
                                                + currentKey.getLocation()
                                                + " because isolation mode = CS or RC or (RR and EOF) and mode = SHARED or UPDATE");
                            }
                            trx.releaseLock(currentKey.getLocation());
                        }
                    }
                }
            } catch (RSSException e) {
                ex = e;
            }

            try {
                bcursor.unfixAll();
            } catch (RSSException e) {
                if (ex == null) {
                    ex = e;
                }
            }
            if (ex != null) {
                throw ex;
            }
            if (stateFetchCompleted != 0) {
                log.warn(LOG_CLASS_NAME, "close", mcat.getMessage("WB0011"));
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

            if (log.isDebugEnabled()) {
                log
                    .debug(
                        LOG_CLASS_NAME,
                        this.getClass().getName() + ".restoreState",
                        "SIMPLEDBM-DEBUG: Current position is set to "
                                + currentKey);
                log.debug(
                    LOG_CLASS_NAME,
                    this.getClass().getName() + ".restoreState",
                    "SIMPLEDBM-DEBUG: Rollback to savepoint is restoring state to "
                            + cs);
            }
            currentKey = cs.currentKey;
            previousKey = cs.previousKey;
            startKey = cs.searchKey;
            pageId = cs.pageId;
            pageLsn = cs.pageLsn;
            fetchCount = cs.fetchCount - 1;
            setEof(cs.eof);
            scanMode = SCAN_FETCH_GREATER_OR_EQUAL;
            fetchNext();
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

        static final class CursorState {
            IndexCursorImpl scan;
            IndexItem currentKey;
            IndexItem previousKey;
            IndexItem searchKey;
            PageId pageId;
            Lsn pageLsn;
            boolean eof;
            int fetchCount;
            int scanMode;

            CursorState(IndexCursorImpl scan) {
                this.scan = scan;
                this.currentKey = scan.currentKey;
                this.previousKey = scan.previousKey;
                this.searchKey = scan.startKey;
                this.pageId = scan.pageId;
                this.pageLsn = scan.pageLsn;
                this.eof = scan.isEof();
                this.fetchCount = scan.fetchCount;
                this.scanMode = scan.scanMode;
            }

            public String toString() {
                return "CursorState(savedCurrentKey = " + currentKey + ")";
            }
        }
    }

    public static final class BTreeCursor {
    	
    	final BTreeIndexManagerImpl btreemgr;

        private BufferAccessBlock q = null;

        private BufferAccessBlock r = null;

        private BufferAccessBlock p = null;

        /**
         * Search key - this is used internally by the various BTree routines.
         */
        public IndexItem searchKey = null;

        /**
         * Used to save the next key location during inserts
         * so that the lock can be released subsequent to the
         * insert.
         */
        private Location nextKeyLocation = null;

        public BTreeCursor(BTreeIndexManagerImpl btreemgr) {
        	this.btreemgr = btreemgr;
        }

        public final BufferAccessBlock getP() {
            return p;
        }

        public final BufferAccessBlock removeP() {
        	if (p != null) {
        		Trace.event(114, p.getPage().getPageId().getContainerId(), p.getPage().getPageId().getPageNumber());
        	}
            BufferAccessBlock bab = p;
            p = null;
            return bab;
        }

        public final void setP(BufferAccessBlock p) {
        	if (p != null) {
        		Trace.event(115, p.getPage().getPageId().getContainerId(), p.getPage().getPageId().getPageNumber());
        	}
            assert this.p == null;
            this.p = p;
        }

        public final BufferAccessBlock getQ() {
            return q;
        }

        public final BufferAccessBlock removeQ() {
        	if (q != null) {
        		Trace.event(116, q.getPage().getPageId().getContainerId(), q.getPage().getPageId().getPageNumber());
        	}
            BufferAccessBlock bab = q;
            q = null;
            return bab;
        }

        public final void setQ(BufferAccessBlock q) {
        	if (q != null) {
        		Trace.event(117, q.getPage().getPageId().getContainerId(), q.getPage().getPageId().getPageNumber());
        	}
            assert this.q == null;
            this.q = q;
        }

        public final BufferAccessBlock getR() {
            return r;
        }

        public final BufferAccessBlock removeR() {
        	if (r != null) {
        		Trace.event(118, r.getPage().getPageId().getContainerId(), r.getPage().getPageId().getPageNumber());
        	}
            BufferAccessBlock bab = r;
            r = null;
            return bab;
        }

        public final void setR(BufferAccessBlock r) {
        	if (r != null) {
        		Trace.event(119, r.getPage().getPageId().getContainerId(), r.getPage().getPageId().getPageNumber());
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
            	Trace.event(120, p.getPage().getPageId().getContainerId(), p.getPage().getPageId().getPageNumber());
                p.unfix();
                p = null;
            }
        }

        public final void unfixQ() {
            if (q != null) {
            	Trace.event(121, q.getPage().getPageId().getContainerId(), q.getPage().getPageId().getPageNumber());
                q.unfix();
                q = null;
            }
        }

        public final void unfixR() {
            if (r != null) {
            	Trace.event(122, r.getPage().getPageId().getContainerId(), r.getPage().getPageId().getPageNumber());
                r.unfix();
                r = null;
            }
        }

        public final void unfixAll() {
            RSSException e = null;
            try {
                unfixP();
            } catch (RSSException e1) {
                e = e1;
            }
            try {
                unfixQ();
            } catch (RSSException e1) {
                if (e == null)
                    e = e1;
            }
            try {
                unfixR();
            } catch (RSSException e1) {
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

    public static final class SearchResult {
        /**
         * The key number can range from {@link BTreeIndexManagerImpl#FIRST_KEY_POS}
         * to {@link BTreeNode#getKeyCount()}. A value of -1 indicates that the
         * search key is greater than all keys in the node.
         */
        int k = -1;
        IndexItem item = null;
        boolean exactMatch = false;
    }

    /**
     * Manages the contents of a B-link tree node. Handles the differences between
     * leaf nodes and index nodes.
     * <pre>
     * Leaf nodes have following structure:
     * [header] [item1] [item2] ... [itemN] [highkey]
     * item[0] = header 
     * item[1,header.KeyCount-1] = keys
     * item[header.keyCount] = high key 
     * The highkey in a leaf node is an extra item, and may or may not be  
     * the same as the last key [itemN] in the page. Operations that change the
     * highkey in leaf pages are Split, Merge and Redistribute. All keys in the
     * page are guaranteed to be &lt;= highkey. 
     * 
     * Index nodes have following structure:
     * [header] [item1] [item2] ... [itemN]
     * item[0] = header 
     * item[1,header.KeyCount] = keys
     * The last key is also the highkey.
     * Note that the rightmost index page at any level has a special
     * key as the highkey - this key has a value of INFINITY. 
     * Each item in an index key contains a pointer to a child page.
     * The child page contains keys that are &lt;= than the item key.
     * </pre>
     * @author Dibyendu Majumdar
     * @since 19-Sep-2005
     */
    public static final class BTreeNode {

        static final short NODE_TYPE_LEAF = 1;
        static final short NODE_TREE_UNIQUE = 2;
        static final short NODE_TREE_DEALLOCATED = 4;

        /**
         * Page being managed.
         */
        SlottedPage page;
        
        /**
         * Noted pageId of page we are handling.
         * Useful for validation.
         */
        PageId pageId;
        
        /**
         * Noted pageLsn of page we are handling.
         * Useful for validation.
         */
        Lsn pageLsn;

        final IndexItemHelper btree;

        /**
         * Cached header.
         */
        BTreeNodeHeader header;

        BTreeNode(IndexItemHelper btree) {
            this.btree = btree;
        }

        private final void sanityCheck() {
        	if (page != null && pageId.equals(page.getPageId()) && pageLsn.equals(page.getPageLsn())) {
        		return;
        	}
        	throw new IndexException("The BTreeNode no longer refers to the correct page");
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
                    new BTreeNodeHeader());
                DiagnosticLogger.log("BTreeNodeHeader=" + header);
                for (int k = FIRST_KEY_POS; k < page.getNumberOfSlots(); k++) {
                    if (page.isSlotDeleted(k)) {
                        continue;
                    }
                    IndexItem item = (IndexItem) page.get(k, getNewIndexItem());
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
						throw new RSSException("Item at slot " + slot + " is in wrong order");
					}
				}
				if (isLeaf()) {
					if (slot == header.keyCount - 1) {
						// key before high key can be equal or less than high key in a leaf page
						IndexItem nextItem = getItem(slot + 1);
						if (thisItem.compareTo(nextItem) > 0) {
							throw new RSSException("Item at slot " + slot
									+ " is in wrong order");
						}
					}
					else if (slot < header.keyCount - 1) {
						IndexItem nextItem = getItem(slot + 1);
						if (thisItem.compareTo(nextItem) >= 0) {
							throw new RSSException("Item at slot " + slot
									+ " is in wrong order");
						}
					}
				}
				else {
					if (slot < header.keyCount) {
						IndexItem nextItem = getItem(slot + 1);
						if (thisItem.compareTo(nextItem) >= 0) {
							throw new RSSException("Item at slot " + slot
									+ " is in wrong order");
						}
					}
				}
			} catch (RSSException e) {
				e.printStackTrace();
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
					throw new RSSException("Page is marked for deallocation");
				}
				if (page.getSpaceMapPageNumber() == -1) {
					throw new RSSException(
							"Page has not been assigned a space amap page");
				}
				if (page.getNumberOfSlots() != header.keyCount + 1) {
					throw new RSSException("Mismatch is keycount");
				}
			} catch (RSSException e) {
				e.printStackTrace();
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
					throw new RSSException("Page is marked for deallocation");
				}
				if (page.getSpaceMapPageNumber() == -1) {
					throw new RSSException(
							"Page has not been assigned a space amap page");
				}
				if (page.getNumberOfSlots() != header.keyCount + 1) {
					throw new RSSException("Mismatch is keycount");
				}
				BTreeNodeHeader header = (BTreeNodeHeader) page.get(
						HEADER_KEY_POS, new BTreeNodeHeader());
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
					IndexItem item = (IndexItem) page.get(k, getNewIndexItem());
					if (prevItem != null) {
						if (k == header.keyCount) {
							if (isLeaf()) {
								if (item.compareTo(prevItem) < 0) {
									throw new RSSException(
											"Item is not in order");
								}
							} else {
								if (item.compareTo(prevItem) <= 0) {
									throw new RSSException(
											"Item is not in order");
								}
							}
						}
						else {
							if (item.compareTo(prevItem) <= 0) {
								throw new RSSException("Item is not in order");
							}
						}
					}
					prevItem = item;
				}
				if (deletedCount > 0) {
					throw new RSSException("Deleted count on page > 0");
				}
				if (keyCount != header.keyCount) {
					throw new RSSException("Mismatch is keycount");
				}
			} catch (RSSException e) {
				e.printStackTrace();
				dumpAsXml();
				throw e;
			}
		}
        
        public final void wrap(SlottedPage page) {
            this.page = page;
            this.pageId = page.getPageId();
            this.pageLsn = page.getPageLsn();
            header = (BTreeNodeHeader) page.get(
                HEADER_KEY_POS,
                new BTreeNodeHeader());
        }

        final BTreeNodeHeader getHeader() {
            return header;
        }

        public final IndexItem getNewIndexItem() {
        	sanityCheck();
        	return new IndexItem(
                btree.getNewIndexKey(),
                btree.getNewLocation(),
                -1,
                isLeaf(),
                isUnique());
        }

        /**
         * Returns the high key. High key is always the last physical key on the page.
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
            return (IndexItem) page.get(slotNumber, getNewIndexItem());
        }

        public final IndexItem getInfiniteKey() {
        	sanityCheck();
        	return new IndexItem(
                btree.getMaxIndexKey(),
                btree.getNewLocation(),
                -1,
                isLeaf(),
                isUnique());
        }

        private void validateItem(IndexItem item) {
        	if (item.isLeaf != isLeaf() || item.isUnique != isUnique()) {
        		throw new RSSException("There is a mismatch between the node and the key type");
        	}
        }
        
        /**
         * Inserts item at specified position. Existing items are shifted
         * to the right if necessary.  
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
         * Returns number of keys stored in the page. For leaf pages, the high key is
         * excluded.
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
         * Finds the key that should be used as the median key when
         * splitting a page. 
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
            log.error(LOG_CLASS_NAME, "getSplitKey", mcat.getMessage(
                "EB0007",
                page));
            throw new IndexException(mcat.getMessage("EB0007", page));
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
         * Searches for the supplied key. If there is an
         * exact match, SearchResult.exactMatch will be set.
         * If a key is found &gt;= 0 the supplied key,
         * SearchResult.k and SearchResult.item will be set to it.
         * If all keys in the page are &lt; search key then, 
         * SearchResult.k will be set to -1 and SearchResult.item will
         * be null. 
         */
        public final SearchResult search(IndexItem key) {
        	sanityCheck();
            SearchResult result = new SearchResult();
            /*
             * Binary search algorithm
             */
            int low = FIRST_KEY_POS;
            int high = getKeyCount();
            while (low <= high) {
                int mid = (low + high) >>> 1;
                if (mid < FIRST_KEY_POS || mid > getKeyCount()) {
                    log.error(LOG_CLASS_NAME, "search", mcat.getMessage(
                        "EB0008",
                        key.toString()));
                    throw new IndexException(mcat.getMessage("EB0008", key
                        .toString()));
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

//		public final SearchResult search_(IndexItem key) {
//			SearchResult result = new SearchResult();
//			int k = 1;
//			// We do not look at the high key when searching
//			for (k = FIRST_KEY_POS; k <= getKeyCount(); k++) {
//				IndexItem item = getItem(k);
//				int comp = item.compareTo(key);
//				if (comp >= 0) {
//					result.k = k;
//					result.item = item;
//					if (comp == 0) {
//						result.exactMatch = true;
//					}
//					break;
//				}
//			}
//			return result;
//		}
//		public final SearchResult searchBinaryDebug(IndexItem key) {
//			SearchResult result = new SearchResult();
//			int k = 1;
//			// We do not look at the high key when searching
//			for (k = FIRST_KEY_POS; k <= getKeyCount(); k++) {
//				IndexItem item = getItem(k);
//				int comp = item.compareTo(key);
//				if (comp >= 0) {
//					result.k = k;
//					result.item = item;
//					if (comp == 0) {
//						result.exactMatch = true;
//					}
//					break;
//				}
//			}
//			SearchResult result1 = new SearchResult();
//			int j = -1;
//			int low = FIRST_KEY_POS;
//			int high = getKeyCount();
//			IndexItem item = null;
//			while (low <= high) {
//				int mid = (low + high) >>> 1;
//				if (mid < FIRST_KEY_POS || mid > getKeyCount()) {
//					throw new IndexException("Invalid binary search result");
//				}
//				item = getItem(mid);
//				int comp = item.compareTo(key);
//				if (comp < 0)
//					low = mid + 1;
//				else if (comp > 0)
//					high = mid - 1;
//				else {
//					// k = mid;
//					// result.exactMatch = true;
//					j = mid;
//					result1.k = j;
//					result1.exactMatch = true;
//					result1.item = item;
////					if (j != result.k) {
////						throw new IndexException("Invalid binary search result");
////					}
//					break;
//				}
//			}
//			if (j == -1) {
//				if (low > getKeyCount()) {
//					j = -1;
//				}
//				else {
//					j = low;
//					result1.k = low;
//					result1.item = getItem(low);
//				}
//			}
//			System.err.println("k = " + result.k + " j = " + j + " low = " + low + " high = " + high + " keyCount = " + getKeyCount());
////			if (result1.k != result.k || result1.item != result.item || result1.exactMatch != result.exactMatch) {
////				System.err.println("k = " + result.k + " j = " + j + " low = " + low + " high = " + high + " keyCount = " + getKeyCount());
////				System.err.println("result.item = " + result.item + " result1.item = " + result1.item);
////				
////				throw new IndexException("Invalid search result for binary search");
////			}
//		
//			return result;
//		}

        /**
         * Finds the child page associated with an index item.
         */
        public final int findChildPage(IndexItem key) {
            if (isLeaf()) {
                log.error(LOG_CLASS_NAME, "findChildPage", mcat.getMessage(
                    "EB0009",
                    page));
                throw new IndexException(mcat.getMessage("EB0009", page));
            }
            int k = 1;
            for (k = FIRST_KEY_POS; k <= getKeyCount(); k++) {
                IndexItem item = getItem(k);
                if (item.compareTo(key) >= 0) {
                    /* Item covers key */
                    return item.getChildPageNumber();
                }
            }
            return -1;
        }

        /**
         * Finds the index item associated with a child page.
         */
        public final IndexItem findIndexItem(int childPageNumber) {
            if (isLeaf()) {
                log.error(LOG_CLASS_NAME, "findIndexItem", mcat.getMessage(
                    "EB0009",
                    page));
                throw new IndexException(mcat.getMessage("EB0009", page));
            }
            for (int k = FIRST_KEY_POS; k <= getKeyCount(); k++) {
                IndexItem item = getItem(k);
                if (item.getChildPageNumber() == childPageNumber) {
                    return item;
                }
            }
            return null;
        }

        /**
         * Finds the index item for left sibling of the
         * specified child page.
         */
        public final IndexItem findPrevIndexItem(int childPageNumber) {
            if (isLeaf()) {
                log.error(LOG_CLASS_NAME, "findPrevIndexItem", mcat.getMessage(
                    "EB0009",
                    page));
                throw new IndexException(mcat.getMessage("EB0009", page));
            }
            IndexItem prev = null;
            for (int k = FIRST_KEY_POS; k <= getKeyCount(); k++) {
                IndexItem item = getItem(k);
                if (item.getChildPageNumber() == childPageNumber) {
                    break;
                }
                prev = item;
            }
            return prev;
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
            // FIXME call a method in slottedpage
            // int requiredSpace = v.getStoredLength() + 6;
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
         * Tests whether this page is about to underflow. 
         * This will be true if it is a root page with only one
         * child or any other page with only two children/keys. 
         * <p>
         */
        public final boolean isAboutToUnderflow() {
        	if (isRoot()) {
        		if (!isLeaf()) {
        			return getKeyCount() == minimumKeys();
        		}
        		else {
        			return false;
        		}
        	}
            return getKeyCount() == minimumKeys();
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
         * Pointer to left sibling page. Note that although we have this,
         * this is not kept fully up-to-date because to do so would require extra
         * work when merging pages. FIXME
         */
        int leftSibling = -1;

        /**
         * Pointer to right sibling page.
         */
        int rightSibling = -1;

        /**
         * Total number of keys present in the page. Includes the high key
         * in leaf pages.
         */
        int keyCount = 0;

        /**
         * Type code for the key factory to be used to manipulate index keys.
         */
        int keyFactoryType = -1;

        /**
         * Typecode of the location factory to be used for generating Location objects.
         */
        int locationFactoryType = -1;

        /* (non-Javadoc)
         * @see org.simpledbm.io.Storable#getStoredLength()
         */
        public final int getStoredLength() {
            return SIZE;
        }

        /* (non-Javadoc)
         * @see org.simpledbm.io.Storable#retrieve(java.nio.ByteBuffer)
         */
        public final void retrieve(ByteBuffer bb) {
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

    }

    /**
     * Base class for all log operations for BTree. A key feature is that
     * all log operations have access to information that allows them to generate
     * keys, locations and index items. Log operations also _know_ whether
     * they are to be applied to leaf pages, or to unique indexes.  
     * <p>
     * Having all the relevant information to hand is useful specially during restart
     * recovery and debugging.
     * The downside is that this information is repeated in all log records.
     * <p>
     * It is possible to avoid storing the index data in log records if we can ensure
     * that the information can be obtained some other way. For example, the
     * Index manager could cache information about indexes, perhaps keyed
     * by container id, and log records could obtain this information by 
     * querying the index manager. However, there is the issue that during 
     * restart recovery, the index may not exist, and therefore at least one
     * log record (the one that creates the index) needs to contain information
     * about the index. 
     * 
     * @author Dibyendu Majumdar
     */
    public static abstract class BTreeLogOperation extends BaseLoggable
            implements ObjectRegistryAware, IndexItemHelper {

        /**
         * Is this a leaf level operation.
         */
        private boolean leaf;

        /**
         * Is this part of a unique index?
         */
        private boolean unique;

        /**
         * Type code for the key factory to be used to manipulate index keys.
         */
        private int keyFactoryType;

        /**
         * Typecode of the location factory to be used for generating Location objects.
         */
        private int locationFactoryType;

        private transient ObjectRegistry objectFactory;

        private transient IndexKeyFactory keyFactory;

        private transient LocationFactory locationFactory;

        @Override
        public int getStoredLength() {
            int n = super.getStoredLength();
            n += 2;
            n += TypeSize.INTEGER * 2;
            return n;
        }

        @Override
        public void retrieve(ByteBuffer bb) {
            super.retrieve(bb);
            keyFactoryType = bb.getInt();
            locationFactoryType = bb.getInt();
            keyFactory = (IndexKeyFactory) objectFactory
                .getInstance(keyFactoryType);
            locationFactory = (LocationFactory) objectFactory
                .getInstance(locationFactoryType);
            leaf = bb.get() == 1;
            unique = bb.get() == 1;
        }

        @Override
        public void store(ByteBuffer bb) {
            super.store(bb);
            bb.putInt(keyFactoryType);
            bb.putInt(locationFactoryType);
            bb.put(leaf ? (byte) 1 : (byte) 0);
            bb.put(unique ? (byte) 1 : (byte) 0);
        }

        public final void setObjectFactory(ObjectRegistry objectFactory) {
            this.objectFactory = objectFactory;
        }

        public final IndexKey getNewIndexKey() {
            return keyFactory.newIndexKey(getPageId().getContainerId());
        }

        public final Location getNewLocation() {
            return locationFactory.newLocation();
        }

        public final IndexKey getMaxIndexKey() {
            return keyFactory.maxIndexKey(getPageId().getContainerId());
        }

        public final IndexItem makeNewItem() {
            return new IndexItem(
                keyFactory.newIndexKey(getPageId().getContainerId()),
                locationFactory.newLocation(),
                -1,
                leaf,
                unique);
        }

        public final void setKeyFactoryType(int keyFactoryType) {
            this.keyFactoryType = keyFactoryType;
            keyFactory = (IndexKeyFactory) objectFactory
                .getInstance(keyFactoryType);
        }

        public final void setLocationFactoryType(int locationFactoryType) {
            this.locationFactoryType = locationFactoryType;
            locationFactory = (LocationFactory) objectFactory
                .getInstance(locationFactoryType);
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

        public final void setUnique(boolean unique) {
            this.unique = unique;
        }

        public final boolean isLeaf() {
            return leaf;
        }

        public final void setLeaf(boolean leaf) {
            this.leaf = leaf;
        }

        final IndexKeyFactory getKeyFactory() {
            return keyFactory;
        }

        final LocationFactory getLocationFactory() {
            return locationFactory;
        }

        protected final ObjectRegistry getObjectFactory() {
            return objectFactory;
        }

        public void copyFrom(BTreeLogOperation other) {
            this.keyFactory = other.keyFactory;
            this.keyFactoryType = other.keyFactoryType;
            this.leaf = other.leaf;
            this.locationFactory = other.locationFactory;
            this.locationFactoryType = other.locationFactoryType;
            this.objectFactory = other.objectFactory;
            this.unique = other.unique;
        }

        public StringBuilder appendTo(StringBuilder sb) {
            
            sb.append("isLeaf=").append(isLeaf() ? "true" : "false");
            sb.append(", isUnique=").append(isUnique() ? "true" : "false");
            sb.append(", keyFactoryType=").append(getKeyFactoryType());
            sb.append(", locationFactoryType=").append(getLocationFactoryType());
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
     * Base class for operations that require a positional update of 
     * a key.
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
                log.error(LOG_CLASS_NAME, "setItem", mcat.getMessage(
                    "EB0010",
                    item));
                throw new IndexException(mcat.getMessage("EB0010", item));
            }
        }

        @Override
        public final void copyFrom(BTreeLogOperation other) {
            super.copyFrom(other);
            KeyUpdateOperation o = (KeyUpdateOperation) other;
            this.item = o.item;
            this.position = o.position;
        }

        @Override
        public final void retrieve(ByteBuffer bb) {
            super.retrieve(bb);
            position = bb.getInt();
            item = new IndexItem(getKeyFactory().newIndexKey(
                getPageId().getContainerId()), getLocationFactory()
                .newLocation(), -1, true, isUnique());
            item.retrieve(bb);
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

        @Override
        public final void init() {
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

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("InsertOperation(");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }
        
        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }
        
    }

    public static final class UndoInsertOperation extends KeyUpdateOperation
            implements Compensation {

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("UndoInsertOperation(");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }
        
        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }
        
    }

    public static final class DeleteOperation extends KeyUpdateOperation
            implements LogicalUndo {
        
        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("DeleteOperation(");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }
        
        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }
        
    }

    public static final class UndoDeleteOperation extends KeyUpdateOperation
            implements Compensation {
        
        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("UndoDeleteOperation(");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }
        
        public String toString() {
            return appendTo(new StringBuilder()).toString();
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
         * The items that will become part of the new sibling page.
         * Includes the highkey in leaf pages.
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
         * After splitting, this is the new keycount of the page.
         * Includes highkey if this is a leaf page.
         */
        short newKeyCount;

        @Override
        public final void init() {
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
        public final void retrieve(ByteBuffer bb) {
            super.retrieve(bb);
            short numberOfItems = bb.getShort();
            items = new LinkedList<IndexItem>();
            for (int i = 0; i < numberOfItems; i++) {
                IndexItem item = new IndexItem(getKeyFactory().newIndexKey(
                    getPageId().getContainerId()), getLocationFactory()
                    .newLocation(), -1, isLeaf(), isUnique());
                item.retrieve(bb);
                items.add(item);
            }
            if (isLeaf()) {
                highKey = new IndexItem(getKeyFactory().newIndexKey(
                    getPageId().getContainerId()), getLocationFactory()
                    .newLocation(), -1, isLeaf(), isUnique());
                highKey.retrieve(bb);
            }
            newSiblingPageNumber = bb.getInt();
            rightSibling = bb.getInt();
            spaceMapPageNumber = bb.getInt();
            newKeyCount = bb.getShort();
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
            for (PageId pageId: getPageIds()) {
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
         * Returns pages that are affected by this log.
         * Included are the page that is being split, and the newly allocated page.
         */
        public final PageId[] getPageIds() {
            return new PageId[] {
                    getPageId(),
                    new PageId(
                        getPageId().getContainerId(),
                        newSiblingPageNumber) };
        }
    }

    public static final class MergeOperation extends BTreeLogOperation
            implements Redoable, MultiPageRedo {

        /**
         * The items that will become part of the new sibling page.
         * Includes the highkey in leaf pages.
         */
        LinkedList<IndexItem> items;

        /**
         * The new sibling page will point to current page's right sibling.
         */
        int rightSibling;

        int rightSiblingSpaceMapPage;

        int rightRightSibling;

        @Override
        public final void init() {
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
        public final void retrieve(ByteBuffer bb) {
            super.retrieve(bb);
            short numberOfItems = bb.getShort();
            items = new LinkedList<IndexItem>();
            for (int i = 0; i < numberOfItems; i++) {
                IndexItem item = new IndexItem(getKeyFactory().newIndexKey(
                    getPageId().getContainerId()), getLocationFactory()
                    .newLocation(), -1, isLeaf(), isUnique());
                item.retrieve(bb);
                items.add(item);
            }
            rightSibling = bb.getInt();
            rightSiblingSpaceMapPage = bb.getInt();
            rightRightSibling = bb.getInt();
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
                    new PageId(
                        getPageId().getContainerId(),
                        rightSiblingSpaceMapPage) };
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("MergeOperation(Number of items=").append(items.size()).append(newline);
            for (IndexItem item : items) {
                sb.append("  item #=[");
                item.appendTo(sb);
                sb.append("]").append(newline);
            }
            sb.append(", rightSibling=").append(rightSibling);
            sb.append(", rightSiblingSpaceMapPage=").append(rightSiblingSpaceMapPage);
            sb.append(", rightRightSibling=").append(rightRightSibling);
            sb.append(", ");
            super.appendTo(sb);
            sb.append(", PageId[]={");
            for (PageId pageId: getPageIds()) {
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

    }

    public static final class LinkOperation extends BTreeLogOperation implements
            Redoable {

        int leftSibling;

        int rightSibling;

        IndexItem leftChildHighKey;

        @Override
        public final int getStoredLength() {
            int n = super.getStoredLength();
            n += leftChildHighKey.getStoredLength();
            n += TypeSize.INTEGER * 2;
            return n;
        }

        @Override
        public final void retrieve(ByteBuffer bb) {
            super.retrieve(bb);
            leftChildHighKey = makeNewItem();
            leftChildHighKey.retrieve(bb);
            rightSibling = bb.getInt();
            leftSibling = bb.getInt();
        }

        @Override
        public final void store(ByteBuffer bb) {
            super.store(bb);
            leftChildHighKey.store(bb);
            bb.putInt(rightSibling);
            bb.putInt(leftSibling);
        }

        @Override
        public final void init() {
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

    }

    /**
     * Log record for the Unlink operation. It is applied to the parent page.
     * @see BTreeImpl#doUnlink(Transaction, BTreeCursor)
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

        @Override
        public final int getStoredLength() {
            int n = super.getStoredLength();
            n += TypeSize.INTEGER * 2;
            return n;
        }

        @Override
        public final void retrieve(ByteBuffer bb) {
            super.retrieve(bb);
            rightSibling = bb.getInt();
            leftSibling = bb.getInt();
        }

        @Override
        public final void store(ByteBuffer bb) {
            super.store(bb);
            bb.putInt(rightSibling);
            bb.putInt(leftSibling);
        }

        @Override
        public final void init() {
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

    }

    /**
     * Unlike the published algorithm we simply transfer one key from the more populated
     * page to the less populated page. 
     * 
     * @author Dibyendu Majumdar
     * @since 06-Oct-2005
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

        @Override
        public final int getStoredLength() {
            int n = super.getStoredLength();
            n += TypeSize.INTEGER * 3;
            n += key.getStoredLength();
            return n;
        }

        @Override
        public final void retrieve(ByteBuffer bb) {
            super.retrieve(bb);
            rightSibling = bb.getInt();
            leftSibling = bb.getInt();
            targetSibling = bb.getInt();
            key = makeNewItem();
            key.retrieve(bb);
        }

        @Override
        public final void store(ByteBuffer bb) {
            super.store(bb);
            bb.putInt(rightSibling);
            bb.putInt(leftSibling);
            bb.putInt(targetSibling);
            key.store(bb);
        }

        @Override
        public final void init() {
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
            for (PageId pageId: getPageIds()) {
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

    }

    /**
     * Log record for IncreaseTreeHeight operation.
     * Must be logged as part of the root page update. The log is applied to the
     * root page and the new child page. It is defined as a Compensation record so that it
     * can be linked back in such a way that if this operation completes, it is treated as 
     * a nested top action.
     * @see BTreeImpl#doIncreaseTreeHeight(Transaction, BTreeCursor)
     * @see BTreeIndexManagerImpl#redoIncreaseTreeHeightOperation(Page, IncreaseTreeHeightOperation)
     */
    public static final class IncreaseTreeHeightOperation extends
            BTreeLogOperation implements Compensation, MultiPageRedo {

        /**
         * These items that will become part of the new left child page.
         * Includes the highkey in leaf pages.
         */
        LinkedList<IndexItem> items;

        /**
         * Root page will contain 2 index entries. First will point to left child,
         * while the second will point to right child.
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

        @Override
        public final void init() {
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
        public final void retrieve(ByteBuffer bb) {
            super.retrieve(bb);
            short numberOfItems = bb.getShort();
            items = new LinkedList<IndexItem>();
            for (int i = 0; i < numberOfItems; i++) {
                IndexItem item = makeNewItem();
                item.retrieve(bb);
                items.add(item);
            }
            numberOfItems = bb.getShort();
            rootItems = new LinkedList<IndexItem>();
            for (int i = 0; i < numberOfItems; i++) {
                IndexItem item = new IndexItem(getKeyFactory().newIndexKey(
                    getPageId().getContainerId()), getLocationFactory()
                    .newLocation(), -1, false, isUnique());
                item.retrieve(bb);
                rootItems.add(item);
            }
            leftSibling = bb.getInt();
            rightSibling = bb.getInt();
            spaceMapPageNumber = bb.getInt();
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
         * This log record will be applioed to the root page and its newly allocated left
         * child.
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
            for (PageId pageId: getPageIds()) {
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
    }

    /**
     * Decrease of the height of the tree when root page has only one child.
     * Must be logged as part of the root page update.
     * @see BTreeImpl#doDecreaseTreeHeight(Transaction, BTreeCursor)
     * @see BTreeIndexManagerImpl#redoDecreaseTreeHeightOperation(Page, DecreaseTreeHeightOperation)
     */
    public static final class DecreaseTreeHeightOperation extends
            BTreeLogOperation implements Redoable, MultiPageRedo {

        /**
         * The items that will become part of the new root page - copied over from child page.
         * Includes the highkey in leaf pages.
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

        @Override
        public final void init() {
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
        public final void retrieve(ByteBuffer bb) {
            super.retrieve(bb);
            short numberOfItems = bb.getShort();
            items = new LinkedList<IndexItem>();
            for (int i = 0; i < numberOfItems; i++) {
                IndexItem item = makeNewItem();
                item.retrieve(bb);
                items.add(item);
            }
            childPageNumber = bb.getInt();
            childPageSpaceMap = bb.getInt();
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
         * This log record will be applied to the root page, and the child page that is to be
         * deallocated. It can also be applied to the space map page, but we handle the
         * space map page update separately in the interest of high concurrency.
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
            for (PageId pageId: getPageIds()) {
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
    }

    /**
     * IndexItem represents an item within a BTree Page. Both Index pages and
     * Leaf pages contain IndexItems. However, the content of IndexItem is somewhat
     * different in Index pages than the content in Leaf pages.
     * <p>
     * In Index pages, an item contains a key, and a child page pointer, plus
     * a location, if the index is non-unique.
     * <p>
     * In leaf pages, an item contains a key and a location.
     * 
     * @author Dibyendu Majumdar
     * @since 18-Sep-2005
     */
    public static final class IndexItem implements Storable,
            Comparable<IndexItem>, Dumpable {

        /**
         * Sortable key
         */
        private IndexKey key;

        /**
         * Location is an optional field; only present in leaf pages and 
         * in non-unique index pages.
         */
        private Location location;

        /**
         * Pointer to child node that has keys <= this key. This is an
         * optional field; only present in index pages. 
         */
        private int childPageNumber;

        /**
         * A non-persistent flag.
         */
        private boolean isLeaf;

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
            this.key = key;
            this.location = loc;
            this.childPageNumber = childPageNumber;
            this.isLeaf = isLeaf;
            this.isUnique = isUnique;
        }

        /* (non-Javadoc)
         * @see org.simpledbm.io.Storable#retrieve(java.nio.ByteBuffer)
         */
        public final void retrieve(ByteBuffer bb) {
            key.retrieve(bb);
            if (isLocationRequired()) {
                location.retrieve(bb);
            }
            if (!isLeaf) {
                childPageNumber = bb.getInt();
            }
        }

        /* (non-Javadoc)
         * @see org.simpledbm.io.Storable#store(java.nio.ByteBuffer)
         */
        public final void store(ByteBuffer bb) {
            key.store(bb);
            if (isLocationRequired()) {
                location.store(bb);
            }
            if (!isLeaf) {
                bb.putInt(childPageNumber);
            }
        }

        /* (non-Javadoc)
         * @see org.simpledbm.io.Storable#getStoredLength()
         */
        public final int getStoredLength() {
            int len = key.getStoredLength();
            if (isLocationRequired()) {
                len += location.getStoredLength();
            }
            if (!isLeaf) {
                len += TypeSize.INTEGER;
            }
            return len;
        }

        public final int compareTo(
                org.simpledbm.rss.impl.im.btree.BTreeIndexManagerImpl.IndexItem o) {
            int comp = key.compareTo(o.key);
            if (comp == 0 && isLocationRequired() && o.isLocationRequired()) {
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

        public final void setKey(IndexKey key) {
            this.key = key;
        }

        public final Location getLocation() {
            return location;
        }

        public final void setLocation(Location location) {
            this.location = location;
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
            if (childPageNumber != other.childPageNumber)
                return false;
            if (isLeaf != other.isLeaf)
                return false;
            if (isUnique != other.isUnique)
                return false;
            if (key == null) {
                if (other.key != null)
                    return false;
            } else if (!key.equals(other.key))
                return false;
            if (location == null) {
                if (other.location != null)
                    return false;
            } else if (!location.equals(other.location))
                return false;
            return true;
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("IndexItem(key=[")
                .append(key)
                .append("], isLeaf=")
                .append(isLeaf)
                .append(", isUnique=")
                .append(isUnique);
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
     * Represents the redo log data for initializing a single BTree node (page) and the corresponding space map page.
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
         * Space Map page that should be updated to reflect the allocated status of the
         * new page.
         */
        private int spaceMapPageNumber;

        @Override
        public final void init() {
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
        public final void retrieve(ByteBuffer bb) {
            super.retrieve(bb);
            short numberOfItems = bb.getShort();
            items = new LinkedList<IndexItem>();
            for (int i = 0; i < numberOfItems; i++) {
                IndexItem item = makeNewItem();
                item.retrieve(bb);
                items.add(item);
            }
            leftSibling = bb.getInt();
            rightSibling = bb.getInt();
            setSpaceMapPageNumber(bb.getInt());
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
                    new PageId(
                        getPageId().getContainerId(),
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

    }

    /**
     * Helper class that reads an XML document and creates LoadPageOperation records using the
     * data in the document. Primary purspose is to help with testing of the
     * BTree algorithms by preparing trees with specific characteristics.
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
                Document document = builder.parse(ClassUtils
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
                        LoadPageOperation loadPageOp;
                        loadPageOp = (LoadPageOperation) btreemgr.loggableFactory
                            .getInstance(
                                BTreeIndexManagerImpl.MODULE_ID,
                                BTreeIndexManagerImpl.TYPE_LOADPAGE_OPERATION);
                        loadPage(loadPageOp, node);
                        records.add(loadPageOp);
                    }
                }
                break;
            }
        }

        private void loadPage(LoadPageOperation loadPageOp, Node page)
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
            loadPageOp.setPageId(btreemgr.spMgr.getPageType(), new PageId(
                containerId,
                pageNumber));
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

        private void loadItem(LoadPageOperation loadPageOp, Node item)
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

            IndexKey key = loadPageOp.getNewIndexKey();
            key.parseString(keyValue);

            Location location = loadPageOp.getLocationFactory().newLocation();
            location.parseString(locationValue);

            loadPageOp.items.add(new IndexItem(
                key,
                location,
                childPageNumber,
                loadPageOp.isLeaf(),
                loadPageOp.isUnique()));
        }

        private void loadHeader(LoadPageOperation loadPageOp, Node header)
                throws Exception {
            NodeList nodes = header.getChildNodes();
            for (int i = 0; i < nodes.getLength(); i++) {
                Node node = nodes.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE
                        && node.getNodeName().equals("unique")) {
                    String value = node.getTextContent();
                    loadPageOp.setUnique(value.equals("yes"));
                } else if (node.getNodeType() == Node.ELEMENT_NODE
                        && node.getNodeName().equals("leaf")) {
                    String value = node.getTextContent();
                    loadPageOp.setLeaf(value.equals("yes"));
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
                    loadPageOp.setSpaceMapPageNumber(Integer.parseInt(value));
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
