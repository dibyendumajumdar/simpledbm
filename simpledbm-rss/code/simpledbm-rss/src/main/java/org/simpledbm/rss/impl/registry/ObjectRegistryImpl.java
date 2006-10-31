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

/**
 * Default implementation of the Object Registry. 
 * 
 * @author Dibyendu Majumdar
 * @since 07-Aug-05
 */
public final class ObjectRegistryImpl implements ObjectRegistry {

    private static final String LOG_CLASS_NAME = ObjectRegistry.class.getName();

    private static final Logger log = Logger.getLogger(ObjectRegistryImpl.class.getPackage().getName());

    /**
     * Maps typecode to ObjectDefinition
     */
    private final HashMap<Short, ObjectDefinition> typeRegistry = new HashMap<Short, ObjectDefinition>();

    /* (non-Javadoc)
     * @see org.simpledbm.common.registry.ObjectFactory#register(int, java.lang.String)
     */
    public synchronized final void register(int tc, String classname) {
        try {
        	if (log.isDebugEnabled()) {
        		log.debug(LOG_CLASS_NAME, "register", "SIMPLEDBM-LOG: Registering typecode " + tc + " for class " + classname);
        	}
            short typecode = (short) tc;
            ObjectDefinition od = typeRegistry.get(typecode);
            if (od != null) {
            	if (!od.isSingleton() && od.getClassName().equals(classname)) {
            		log.warn(LOG_CLASS_NAME, "register", "SIMPLEDBM-LOG: Duplicate registration of type " + typecode + " ignored");
                    return;
            	}
                throw new ObjectCreationException(
                        "SIMPLEDBM-ERROR: Duplicated registration of type " + typecode + " does not match previous registration");
            }
            Class clazz = ClassUtils.forName(classname);
            typeRegistry.put(typecode, new ObjectDefinition(clazz, typecode));
        } catch (ClassNotFoundException e) {
            throw new ObjectCreationException(
                    "SIMPLEDBM-ERROR: Error instantiating class "
                            + classname, e);
        }
    }

    /* (non-Javadoc)
     * @see org.simpledbm.common.registry.ObjectFactory#register(int, java.lang.Object)
     */
    public synchronized final void register(int tc, Object object) {
        short typecode = (short) tc;
    	if (log.isDebugEnabled()) {
    		log.debug(LOG_CLASS_NAME, "register", "SIMPLEDBM-LOG: Registering typecode " + tc + " for Singleton " + object);
    	}
        ObjectDefinition od = typeRegistry.get(typecode);
        if (od != null) {
        	if (od.isSingleton() && od.getClassName().equals(object.getClass().getName())) {
        		log.warn(LOG_CLASS_NAME, "register", "SIMPLEDBM-LOG: Duplicate registration of type " + typecode + " ignored");
                return;
        	}
            throw new ObjectCreationException(
                    "SIMPLEDBM-ERROR: Duplicated registration of type " + typecode + " does not match previous registration");
        }
        typeRegistry.put(typecode, new ObjectDefinition(object, typecode));
    }

    /* (non-Javadoc)
     * @see org.simpledbm.common.registry.ObjectFactory#getInstance(int)
     */
    public Object getInstance(int typecode) {
        try {
            ObjectDefinition od = typeRegistry.get((short) typecode);
            if (od == null) {
                throw new ObjectCreationException.UnknownTypeException("SIMPLEDBM-ERROR: Error creating instance of type "
                            + typecode + ", type not found");
            }
            if (od.isSingleton()) {
                return od.getInstance();
            }
            else {
                Object o = od.getInstance();
                if (o instanceof ObjectRegistryAware) {
                    ObjectRegistryAware ofa = (ObjectRegistryAware) o;
                    ofa.setObjectFactory(this);
                }
                return o;
            }
        } catch (ObjectCreationException e) {
        	throw e;
        } catch (Exception e) {
            throw new ObjectCreationException(
                    "SIMPLEDBM-ERROR: Error creating instance of typecode "
                            + typecode, e);
        }
    }

    /**
     * Holds the definition of a type, either its class or if it is
     * a simgleton, then the object itself.
     */
    static class ObjectDefinition {
        private final Object object;
        private final int typeCode;
        private final boolean singleton;
        
        ObjectDefinition(Object object, int typecode) {
            this.typeCode = typecode;
            this.object = object;
            singleton = !(object instanceof Class);
        }

        final Object getInstance() throws InstantiationException, IllegalAccessException {
            if (singleton)
                return object;
            else {
                return ((Class) object).newInstance();
            }
        }

        final boolean isSingleton() {
            return singleton;
        }

        final int getTypeCode() {
            return typeCode;
        }
        
        final String getClassName() {
        	if (singleton) {
        		return object.getClass().getName();
        	}
        	else {
        		return ((Class) object).getName();
        	}
        }
    }
}
