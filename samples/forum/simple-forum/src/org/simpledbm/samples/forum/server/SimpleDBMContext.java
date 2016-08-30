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
package org.simpledbm.samples.forum.server;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.simpledbm.network.client.api.SessionManager;

public class SimpleDBMContext {

    final static int TABLE_SEQUENCE = 7;
    final static int TABLE_TOPIC = 3;
    final static int TABLE_FORUM = 1;
    final static int TABLE_POST = 5;

    final static int TOPIC_FORUM_NAME = 0;
    final static int TOPIC_TOPIC_ID = 1;
    final static int TOPIC_TITLE = 2;
    final static int TOPIC_STARTED_BY = 3;
    
    final static int FORUM_NAME = 0;
    final static int FORUM_DESCRIPTION = 1;
    
    final static int POST_FORUM_NAME = 0;
    final static int POST_TOPIC_ID = 1;
    final static int POST_POST_ID = 2;
    final static int POST_AUTHOR = 3;
    final static int POST_DATE_TIME = 4;
    final static int POST_CONTENT = 5;
    
    final SessionManager sm;

    SimpleDBMContext(Properties properties) {
        System.err.println("Connecting to SimpleDBM server");
        sm = SessionManager.getSessionManager(properties, "localhost", 8000,
                (int) TimeUnit.MILLISECONDS.convert(5 * 60, TimeUnit.SECONDS));
    }

    void destroy() {
        System.err.println("Terminating connection to SimpleDBM server");
        if (sm != null) {
            sm.getConnection().close();
        }
    }

    SessionManager getSessionManager() {
        return sm;
    }

}
