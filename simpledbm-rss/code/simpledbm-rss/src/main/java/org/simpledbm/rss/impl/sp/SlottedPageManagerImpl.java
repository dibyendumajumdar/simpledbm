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
/*
 * Created on: Nov 17, 2005
 * Author: Dibyendu Majumdar
 */
package org.simpledbm.rss.impl.sp;

import java.util.Properties;

import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.api.registry.ObjectRegistry;
import org.simpledbm.common.util.mcat.MessageCatalog;
import org.simpledbm.rss.api.pm.PageManager;
import org.simpledbm.rss.api.sp.SlottedPageManager;

public class SlottedPageManagerImpl implements SlottedPageManager {

    static final short MODULE_ID = 3;

    static final short TYPE_BASE = 15;
    static final short TYPE_SLOTTEDPAGE = TYPE_BASE + 1;
    
    final Platform platform;
    
    final PlatformObjects po;

    public SlottedPageManagerImpl(Platform platform, ObjectRegistry objectFactory, PageManager pageFactory, Properties p) {
    	this.platform = platform;
    	this.po = platform.getPlatformObjects(SlottedPageManager.LOGGER_NAME);
    	MessageCatalog mcat = po.getMessageCatalog();
        // Slotted Page Manager
        mcat.addMessage(
                "EO0001",
                "SIMPLEDBM-EO0001: Cannot insert item {0} into page {1} due to lack of space");
        mcat.addMessage(
                "EO0002",
                "SIMPLEDBM-EO0002: Cannot insert item {0} of size {3} into page at slot {2} due to lack of space: page contents = {1}");
        mcat.addMessage(
                "EO0003",
                "SIMPLEDBM-EO0003: Cannot access item at slot {0}, number of slots in page is {1}");
        mcat.addMessage(
                "EO0004",
                "SIMPLEDBM-EO0004: Cannot modify page contents as page is not latched EXCLUSIVELY");
        mcat.addMessage("EO0005", "SIMPLEDBM-EO0005: Invalid page contents");
        mcat.addMessage(
                "EO0006",
                "SIMPLEDBM-EO0006: Unexpected error: failed to find insertion point in page");
        objectFactory.registerSingleton(TYPE_SLOTTEDPAGE, new SlottedPageImpl.SlottedPageImplFactory(po, pageFactory));
    }

    public int getPageType() {
        return TYPE_SLOTTEDPAGE;
    }

}
