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
package org.simpledbm.rss.impl.registry;

import java.util.HashMap;

import org.simpledbm.rss.api.registry.ObjectCreationException;
import org.simpledbm.rss.api.registry.ObjectRegistry;
import org.simpledbm.rss.api.registry.ObjectRegistryAware;
import org.simpledbm.rss.util.ClassUtils;
import org.simpledbm.rss.util.logging.Logger;
import org.simpledbm.rss.util.mcat.MessageCatalog;

/**
 * Default implementation of the Object Registry.
 * 
 * @author Dibyendu Majumdar
 * @since 07-Aug-05
 */
public final class ObjectRegistryImpl implements ObjectRegistry {

	private static final String LOG_CLASS_NAME = ObjectRegistry.class.getName();

	private static final Logger log = Logger.getLogger(ObjectRegistryImpl.class
			.getPackage().getName());

	/**
	 * Maps typecode to ObjectDefinition
	 */
	private final HashMap<Short, ObjectDefinition> typeRegistry = new HashMap<Short, ObjectDefinition>();

	private static final MessageCatalog mcat = new MessageCatalog();

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.simpledbm.common.registry.ObjectFactory#register(int,
	 *      java.lang.String)
	 */
	public synchronized final void register(int tc, String classname) {
		try {
			if (log.isDebugEnabled()) {
				log.debug(this.getClass().getName(), "register",
						"SIMPLEDBM-DEBUG: Registering typecode " + tc
								+ " for class " + classname);
			}
			short typecode = (short) tc;
			ObjectDefinition od = typeRegistry.get(typecode);
			if (od != null) {
				if (!od.isSingleton() && od.getClassName().equals(classname)) {
					log.warn(LOG_CLASS_NAME, "register", mcat.getMessage(
							"WR0001", typecode));
					return;
				}
				log.error(this.getClass().getName(), "register", mcat
						.getMessage("ER0002", typecode, od.getClassName(),
								classname));
				throw new ObjectCreationException(mcat.getMessage("ER0002",
						typecode, od.getClassName(), classname));
			}
			Class clazz = ClassUtils.forName(classname);
			typeRegistry.put(typecode, new ClassDefinition(typecode, clazz));
		} catch (ClassNotFoundException e) {
			log.error(this.getClass().getName(), "register", mcat.getMessage("ER0005", classname), e);
			throw new ObjectCreationException(mcat.getMessage("ER0005", classname), e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.simpledbm.common.registry.ObjectFactory#register(int,
	 *      java.lang.Object)
	 */
	public synchronized final void register(int tc, Object object) {
		short typecode = (short) tc;
		if (log.isDebugEnabled()) {
			log.debug(this.getClass().getName(), "register",
					"SIMPLEDBM-DEBUG: Registering typecode " + tc
							+ " for Singleton " + object);
		}
		ObjectDefinition od = typeRegistry.get(typecode);
		if (od != null) {
			if (od.isSingleton() && od.getInstance() == object) {
				log.warn(LOG_CLASS_NAME, "register", mcat.getMessage("WR0003",
						typecode));
				return;
			}
			log.error(this.getClass().getName(), "register", mcat.getMessage(
					"ER0004", typecode, od.getInstance(), object));
			throw new ObjectCreationException(mcat.getMessage("ER0004",
					typecode, od.getInstance(), object));
		}
		typeRegistry.put(typecode, new SingletonObjectDefinition(typecode,
				object));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.simpledbm.common.registry.ObjectFactory#getInstance(int)
	 */
	public Object getInstance(int typecode) {
		ObjectDefinition od = typeRegistry.get((short) typecode);
		if (od == null) {
			log.error(this.getClass().getName(), "getInstance", mcat.getMessage("ER0006", typecode));
			throw new ObjectCreationException.UnknownTypeException(mcat.getMessage("ER0006", typecode));
		}
		if (od.isSingleton()) {
			return od.getInstance();
		} else {
			Object o = od.getInstance();
			if (o instanceof ObjectRegistryAware) {
				ObjectRegistryAware ofa = (ObjectRegistryAware) o;
				ofa.setObjectFactory(this);
			}
			return o;
		}
	}

	// /**
	// * Holds the definition of a type, either its class or if it is
	// * a simgleton, then the object itself.
	// */
	// static class ObjectDefinition {
	// private final Object object;
	// private final int typeCode;
	// private final boolean singleton;
	//        
	// ObjectDefinition(Object object, int typecode) {
	// this.typeCode = typecode;
	// this.object = object;
	// singleton = !(object instanceof Class);
	// }
	//
	// final Object getInstance() {
	// if (singleton)
	// return object;
	// else {
	// try {
	// return ((Class) object).newInstance();
	// } catch (Exception e) {
	// // TODO Auto-generated catch block
	// throw new ObjectCreationException(e);
	// }
	// }
	// }
	//
	// final boolean isSingleton() {
	// return singleton;
	// }
	//
	// final int getTypeCode() {
	// return typeCode;
	// }
	//        
	// final String getClassName() {
	// if (singleton) {
	// return object.getClass().getName();
	// }
	// else {
	// return ((Class) object).getName();
	// }
	// }
	// }

	/**
	 * Holds the definition of a type, either its class or if it is a simgleton,
	 * then the object itself.
	 */
	static abstract class ObjectDefinition {
		private final int typeCode;

		ObjectDefinition(int typecode) {
			this.typeCode = typecode;
		}

		final int getTypeCode() {
			return typeCode;
		}

		abstract Object getInstance();

		abstract boolean isSingleton();

		abstract String getClassName();
	}

	static class SingletonObjectDefinition extends ObjectDefinition {
		final Object object;

		SingletonObjectDefinition(int typecode, Object object) {
			super(typecode);
			this.object = object;
		}

		@Override
		String getClassName() {
			return object.getClass().getName();
		}

		@Override
		Object getInstance() {
			return object;
		}

		@Override
		boolean isSingleton() {
			return true;
		}
	}

	static class ClassDefinition extends ObjectDefinition {

		final Class clazz;

		ClassDefinition(int typecode, Class klass) {
			super(typecode);
			this.clazz = klass;
		}

		@Override
		String getClassName() {
			return clazz.getName();
		}

		@Override
		Object getInstance() {
			try {
				return clazz.newInstance();
			} catch (Exception e) {
				log.error(this.getClass().getName(), "getInstance", mcat.getMessage("ER0007", getTypeCode(), clazz), e);
				throw new ObjectCreationException(mcat.getMessage("ER0007", getTypeCode(), clazz), e);
			}
		}

		@Override
		boolean isSingleton() {
			return false;
		}

	}

}
