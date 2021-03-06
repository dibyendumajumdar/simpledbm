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
 * Created on: Nov 17, 2005
 * Author: Dibyendu Majumdar
 */
package org.simpledbm.rss.impl.sp;

import java.util.Properties;

import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.api.registry.ObjectRegistry;
import org.simpledbm.rss.api.pm.PageManager;
import org.simpledbm.rss.api.sp.SlottedPageManager;

public class SlottedPageManagerImpl implements SlottedPageManager {

    static final short MODULE_ID = 3;

    static final short TYPE_BASE = 15;
    static final short TYPE_SLOTTEDPAGE = TYPE_BASE + 1;

    final Platform platform;

    final PlatformObjects po;

    public SlottedPageManagerImpl(Platform platform,
            ObjectRegistry objectFactory, PageManager pageFactory, Properties p) {
        this.platform = platform;
        this.po = platform.getPlatformObjects(SlottedPageManager.LOGGER_NAME);
        objectFactory.registerSingleton(TYPE_SLOTTEDPAGE,
                new SlottedPageImpl.SlottedPageImplFactory(po, pageFactory));
    }

    public int getPageType() {
        return TYPE_SLOTTEDPAGE;
    }

}
