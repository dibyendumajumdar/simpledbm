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
/*
 * Created on: Nov 17, 2005
 * Author: Dibyendu Majumdar
 */
package org.simpledbm.rss.impl.sp;

import java.util.Properties;

import org.simpledbm.rss.api.pm.PageManager;
import org.simpledbm.rss.api.registry.ObjectRegistry;
import org.simpledbm.rss.api.sp.SlottedPageManager;

public class SlottedPageManagerImpl implements SlottedPageManager {

    static final short MODULE_ID = 3;

    static final short TYPE_BASE = 15;
    static final short TYPE_SLOTTEDPAGE = TYPE_BASE + 1;

    public SlottedPageManagerImpl(ObjectRegistry objectFactory, PageManager pageFactory, Properties p) {
        objectFactory.registerSingleton(TYPE_SLOTTEDPAGE, new SlottedPageImpl.SlottedPageImplFactory(pageFactory));
    }

    public int getPageType() {
        return TYPE_SLOTTEDPAGE;
    }

}
