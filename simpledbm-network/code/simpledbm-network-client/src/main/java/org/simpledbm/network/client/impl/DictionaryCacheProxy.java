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
package org.simpledbm.network.client.impl;

import org.simpledbm.network.common.api.QueryDictionaryMessage;
import org.simpledbm.network.common.api.RequestCode;
import org.simpledbm.network.nio.api.Connection;
import org.simpledbm.network.nio.api.Response;
import org.simpledbm.typesystem.api.DictionaryCache;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.api.TypeFactory;

public class DictionaryCacheProxy implements DictionaryCache {

    SessionManagerImpl sessionManager;
    TypeFactory typeFactory;

    public DictionaryCacheProxy(SessionManagerImpl sessionManager,
            Connection connection, TypeFactory typeFactory) {
        this.sessionManager = sessionManager;
        this.typeFactory = typeFactory;
    }

    public TypeDescriptor[] getTypeDescriptor(int containerId) {
        //		System.err.println("Sending query to server");
        QueryDictionaryMessage message = new QueryDictionaryMessage(containerId);
        Response response = sessionManager.sendMessage(0,
                RequestCode.QUERY_DICTIONARY, message);
        TypeDescriptor[] td = typeFactory.retrieve(response.getData());
        return td;
    }

    public void registerRowType(int containerId, TypeDescriptor[] rowTypeDesc) {
        throw new UnsupportedOperationException();
    }

    public void unregisterRowType(int containerId) {
        throw new UnsupportedOperationException();
    }

}
