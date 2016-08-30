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
package org.simpledbm.rss.impl.pm;

import java.nio.ByteBuffer;
import java.util.Properties;

import org.simpledbm.common.api.exception.ExceptionHandler;
import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.api.registry.ObjectRegistry;
import org.simpledbm.common.util.ChecksumCalculator;
import org.simpledbm.common.util.TypeSize;
import org.simpledbm.common.util.logging.Logger;
import org.simpledbm.common.util.mcat.Message;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.common.util.mcat.MessageType;
import org.simpledbm.rss.api.latch.LatchFactory;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageException;
import org.simpledbm.rss.api.pm.PageFactory;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.pm.PageManager;
import org.simpledbm.rss.api.pm.PageReadException;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageManager;

/**
 * The PageManager handles the reading and writing of pages. The actual
 * reading/writing of a particular Page type is delegated to appropriate
 * PageFactory implementation.
 * 
 * @author Dibyendu Majumdar
 * @since 14-Aug-2005
 */
public final class PageManagerImpl implements PageManager {

    static final int MODULE_ID = 5;

    static final int TYPE_BASE = 10;
    static final int TYPE_RAW_PAGE = TYPE_BASE + 1;

    final Logger log;

    final ExceptionHandler exceptionHandler;

    /**
     * Default page size is 8 KB.
     */
    private static final int DEFAULT_PAGE_SIZE = 8 * 1024;

    /**
     * The size of all pages managed by the PageManager is fixed at the time of
     * construction.
     */
    private final int pageSize; // default page size is 8K

    /**
     * An ObjectRegistry instance is required for obtaining access to
     * PageFactory implementations for various page types.
     */
    private final ObjectRegistry objectRegistry;

    /**
     * StorageManager provides access to the StorageContainers by their
     * container Id.
     */
    private final StorageManager storageManager;

    /**
     * LatchFactory is used to create latches that are assigned to pages.
     */
    private final LatchFactory latchFactory;

    // Page Manager messages
    static Message m_EP0001 = new Message(
            'R',
            'P',
            MessageType.ERROR,
            1,
            "Error occurred while reading page {0}: the number of bytes read is {1}; but expected {2} bytes");
    static Message m_EP0002 = new Message('R', 'P', MessageType.ERROR, 2,
            "Error occurred while reading page {0}: container not available");
    static Message m_EP0003 = new Message('R', 'P', MessageType.ERROR, 3,
            "Error occurred while writing page {0}: container not available");
    static Message m_EP0004 = new Message('R', 'P', MessageType.ERROR, 4,
            "Error occurred while reading page {0}: checksum invalid");
    static Message m_EP0005 = new Message('R', 'P', MessageType.ERROR, 5,
            "A PageFactory was not available to handle page type {0}");

    public PageManagerImpl(Platform platform, int pageSize,
            ObjectRegistry objectRegistry, StorageManager storageManager,
            LatchFactory latchFactory, Properties p) {
        PlatformObjects po = platform
                .getPlatformObjects(PageManager.LOGGER_NAME);
        this.log = po.getLogger();
        this.exceptionHandler = po.getExceptionHandler();
        this.pageSize = pageSize;
        this.objectRegistry = objectRegistry;
        this.storageManager = storageManager;
        this.latchFactory = latchFactory;
        objectRegistry.registerSingleton(TYPE_RAW_PAGE,
                new RawPage.RawPageFactory(this));
    }

    public PageManagerImpl(Platform platform, ObjectRegistry objectRegistry,
            StorageManager storageManager, LatchFactory latchFactory,
            Properties p) {
        this(platform, DEFAULT_PAGE_SIZE, objectRegistry, storageManager,
                latchFactory, p);
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.pm.PageManager#getPageSize()
     */
    public final int getPageSize() {
        return pageSize;
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.pm.PageManager#getUsablePageSize()
     */
    public final int getUsablePageSize() {
        /*
         * The usable page size is somewhat less than the actual page size, because
         * we use 4 bytes to store a checksum in the page.
         */
        return pageSize - TypeSize.LONG;
    }

    private PageFactory getPageFactory(int pagetype) {
        PageFactory pf = (PageFactory) objectRegistry.getSingleton(pagetype);
        if (null == pf) {
            exceptionHandler.errorThrow(this.getClass(),
                    "getPageFactory", new PageException(new MessageInstance(
                            m_EP0005, pagetype)));
        }
        return pf;
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.pm.PageFactory#getInstance(int, org.simpledbm.rss.api.pm.PageId)
     */
    public final Page getInstance(int pagetype, PageId pageId) {
        PageFactory pf = getPageFactory(pagetype);
        Page page = pf.getInstance(pagetype, pageId);
        return page;
    }

    /**
     * Unmarshalls a byte stream to a page. First two bytes must contain the
     * page type information for the page. This is used to obtain the correct
     * PageFactory implementation from the Object Registry.
     * 
     * @param pageId The ID of the page to be unmarshalled
     * @param bb The ByteBuffer that provides access to the byte stream
     * @return Page instance initialized with the contents of the byte stream
     */
    private Page getInstance(PageId pageId, ByteBuffer bb) {
        /*
         * Get the page type
         */
        bb.mark();
        short pagetype = bb.getShort();
        bb.reset();
        /*
         * Obtain the relevant PageFactory implementation
         */
        PageFactory pf = getPageFactory(pagetype);
        /*
         * Instantiate the page
         */
        Page page = pf.getInstance(pageId, bb);
        return page;
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.pm.PageFactory#retrieve(org.simpledbm.rss.api.pm.PageId)
     */
    public final Page retrieve(PageId pageId) {
        StorageContainer container = storageManager.getInstance(pageId
                .getContainerId());
        if (container == null) {
            exceptionHandler.errorThrow(this.getClass(), "retrieve",
                    new PageException(new MessageInstance(m_EP0002, pageId)));
        }
        long offset = pageId.getPageNumber() * pageSize;
        byte[] data = new byte[pageSize];
        int n = container.read(offset, data, 0, pageSize);
        if (n != pageSize) {
            exceptionHandler.errorThrow(this.getClass(), "retrieve",
                    new PageReadException(new MessageInstance(m_EP0001, pageId,
                            n, pageSize)));
        }
        /*
         * The first 4 bytes of a page contains a checksum calculated over the
         * rest of the page data. We need to validate that the checksum obtained
         * from the page matches the calculated checksum of the page data.
         */
        long checksumCalculated = ChecksumCalculator.compute(data,
                TypeSize.LONG, pageSize - TypeSize.LONG);
        ByteBuffer bb = ByteBuffer.wrap(data);
        long checksumOnPage = bb.getLong();
        if (checksumOnPage != checksumCalculated) {
            exceptionHandler
                    .errorThrow(this.getClass(), "retrieve",
                            new PageReadException(new MessageInstance(m_EP0004,
                                    pageId)));
        }
        return getInstance(pageId, bb);
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.pm.PageFactory#store(org.simpledbm.rss.api.pm.Page)
     */
    public final void store(Page page) {
        StorageContainer container = storageManager.getInstance(page
                .getPageId().getContainerId());
        if (container == null) {
            exceptionHandler.errorThrow(this.getClass(), "retrieve",
                    new PageException(new MessageInstance(m_EP0003, page
                            .getPageId())));
        }

        byte[] data = new byte[pageSize];
        ByteBuffer bb = ByteBuffer.wrap(data);
        bb.mark();
        /*
         * The first 4 bytes of a page contains a checksum calculated over the
         * rest of the page data. 
         * As we do not know the checksum yet, we insert a place holder.
         */
        bb.putLong(0);
        page.store(bb);
        /*
         * Calculate the checksum and store it in the first 4 bytes.
         */
        long checksum = ChecksumCalculator.compute(data, TypeSize.LONG,
                pageSize - TypeSize.LONG);
        bb.reset();
        bb.putLong(checksum);
        /*
         * Now we can persist the page.
         */
        long offset = page.getPageId().getPageNumber() * pageSize;
        container.write(offset, data, 0, pageSize);
    }

    public int getRawPageType() {
        return TYPE_RAW_PAGE;
    }

    public LatchFactory getLatchFactory() {
        return latchFactory;
    }

}
