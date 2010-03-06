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
	
	public DictionaryCacheProxy(SessionManagerImpl sessionManager, Connection connection, TypeFactory typeFactory) {
		this.sessionManager = sessionManager;
		this.typeFactory = typeFactory;
	}

	public TypeDescriptor[] getTypeDescriptor(int containerId) {
//		System.err.println("Sending query to server");
        QueryDictionaryMessage message = new QueryDictionaryMessage(containerId);
        Response response = sessionManager.sendMessage(0, RequestCode.QUERY_DICTIONARY, message);
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
