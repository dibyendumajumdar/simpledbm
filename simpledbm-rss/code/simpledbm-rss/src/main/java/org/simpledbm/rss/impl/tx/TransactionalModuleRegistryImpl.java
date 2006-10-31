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
 *    Email  : dibyendu@mazumdar.demon.co.uk
 */
package org.simpledbm.rss.impl.tx;

import java.util.HashMap;

import org.simpledbm.rss.api.tx.TransactionalModule;
import org.simpledbm.rss.api.tx.TransactionalModuleRegistry;

public final class TransactionalModuleRegistryImpl implements TransactionalModuleRegistry {

	private final HashMap<Short, TransactionalModule> moduleMap = new HashMap<Short, TransactionalModule>();
	
	public final synchronized void registerModule(int moduleId, TransactionalModule module) {
		moduleMap.put((short)moduleId, module);
	}

	public final TransactionalModule getModule(int moduleId) {
		TransactionalModule module = moduleMap.get((short)moduleId);
		if (module == null) {
			// TODO Use proper type
			throw new RuntimeException();
		}
		return module;
	}

}
