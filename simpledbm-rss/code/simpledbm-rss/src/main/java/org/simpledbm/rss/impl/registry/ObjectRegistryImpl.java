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
package org.simpledbm.rss.impl.registry;

import java.nio.ByteBuffer;
import java.util.Properties;

import org.simpledbm.rss.api.registry.ObjectCreationException;
import org.simpledbm.rss.api.registry.ObjectFactory;
import org.simpledbm.rss.api.registry.ObjectRegistry;
import org.simpledbm.rss.util.TypeSize;
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
        .getPackage()
        .getName());
    
    static final class TypeRegistry {
        final ObjectDefinition[] typeRegistry = new ObjectDefinition[Short.MAX_VALUE];

        final void put(int tc, ObjectDefinition def) {
        	typeRegistry[tc] = def;
        }
        
        final ObjectDefinition get(int tc) {
        	return typeRegistry[tc];
        }
    }

    /**
     * Maps typecode to ObjectDefinition
     */
//    private final HashMap<Short, ObjectDefinition> typeRegistry = new HashMap<Short, ObjectDefinition>();
    TypeRegistry typeRegistry = new TypeRegistry();
    
    private static final MessageCatalog mcat = new MessageCatalog();

    public ObjectRegistryImpl(Properties properties) {
    }
    
    public synchronized final void registerType(int tc,
			ObjectFactory objectFactory) {
    	assert tc < Short.MAX_VALUE && tc > 0;
		String classname = objectFactory.getType().getName();
		if (log.isDebugEnabled()) {
			log.debug(this.getClass().getName(), "register",
					"SIMPLEDBM-DEBUG: Registering typecode " + tc
							+ " for class " + classname);
		}
		short typecode = (short) tc;
		ObjectDefinition od = typeRegistry.get(typecode);
		if (od != null) {
			if (!od.isSingleton() && od.getClassName().equals(classname)) {
				log.warn(LOG_CLASS_NAME, "register", mcat.getMessage("WR0001",
						typecode));
				return;
			}
			log.error(this.getClass().getName(), "register", mcat.getMessage(
					"ER0002", typecode, od.getClassName(), classname));
			throw new ObjectCreationException(mcat.getMessage("ER0002",
					typecode, od.getClassName(), classname));
		}
		typeRegistry.put(typecode, new FactoryObjectDefinition(typecode,
				objectFactory));
	}

    
    public synchronized final void registerSingleton(int tc, Object object) {
    	assert tc < Short.MAX_VALUE && tc > 0;
        short typecode = (short) tc;
        if (log.isDebugEnabled()) {
            log.debug(
                this.getClass().getName(),
                "register",
                "SIMPLEDBM-DEBUG: Registering typecode " + tc
                        + " for Singleton " + object);
        }
        ObjectDefinition od = typeRegistry.get(typecode);
        if (od != null) {
            if (od.isSingleton() && od.getInstance() == object) {
                log.warn(LOG_CLASS_NAME, "register", mcat.getMessage(
                    "WR0003",
                    typecode));
                return;
            }
            log.error(this.getClass().getName(), "register", mcat.getMessage(
                "ER0004",
                typecode,
                od.getInstance(),
                object));
            throw new ObjectCreationException(mcat.getMessage(
                "ER0004",
                typecode,
                od.getInstance(),
                object));
        }
        typeRegistry.put(typecode, new SingletonObjectDefinition(
            typecode,
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
            log.error(this.getClass().getName(), "getInstance", mcat
                .getMessage("ER0006", typecode));
            throw new ObjectCreationException.UnknownTypeException(mcat
                .getMessage("ER0006", typecode));
        }
        return od.getInstance();
    }
    
    public Object getInstance(int typecode, ByteBuffer buf) {
        ObjectDefinition od = typeRegistry.get((short) typecode);
        if (od == null) {
            log.error(this.getClass().getName(), "getInstance", mcat
                .getMessage("ER0006", typecode));
            throw new ObjectCreationException.UnknownTypeException(mcat
                .getMessage("ER0006", typecode));
        }
        return od.getInstance(buf);
	}

	public Object getInstance(ByteBuffer buf) {
		if (buf.remaining() < TypeSize.SHORT) {
			throw new IllegalArgumentException();
		}
		buf.mark();
		short type = buf.getShort();
		buf.reset();
		return getInstance(type, buf);
	}

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

        abstract Object getInstance(ByteBuffer buf);
        
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

		@Override
		Object getInstance(ByteBuffer buf) {
			throw new UnsupportedOperationException();
		}
    }

    static class FactoryObjectDefinition extends ObjectDefinition {

    	ObjectFactory objectFactory;
    	
		public FactoryObjectDefinition(int typecode, ObjectFactory objectFactory) {
			super(typecode);
			this.objectFactory = objectFactory;
		}

		@Override
		String getClassName() {
			return objectFactory.getType().getName();
		}

		@Override
		Object getInstance() {
			return objectFactory.newInstance();
		}

		@Override
		Object getInstance(ByteBuffer buf) {
			return objectFactory.newInstance(buf);
		}

		@Override
		boolean isSingleton() {
			return false;
		}
    }   
}
