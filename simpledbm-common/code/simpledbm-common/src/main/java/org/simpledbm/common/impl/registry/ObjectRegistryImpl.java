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
package org.simpledbm.common.impl.registry;

import java.nio.ByteBuffer;
import java.util.Properties;

import org.simpledbm.common.api.exception.ExceptionHandler;
import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.api.registry.ObjectCreationException;
import org.simpledbm.common.api.registry.ObjectFactory;
import org.simpledbm.common.api.registry.ObjectRegistry;
import org.simpledbm.common.util.TypeSize;
import org.simpledbm.common.util.logging.Logger;
import org.simpledbm.common.util.mcat.Message;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.common.util.mcat.MessageType;

/**
 * Default implementation of the Object Registry.
 * 
 * @author Dibyendu Majumdar
 * @since 07-Aug-05
 */
public final class ObjectRegistryImpl implements ObjectRegistry {

    private final Logger log;

    private final ExceptionHandler exceptionHandler;

    @SuppressWarnings("unused")
    private final Platform platform;

    // Object Registry messages
    static Message m_WR0001 = new Message('C', 'R', MessageType.WARN, 1,
            "Duplicate registration of type {0} ignored");
    static Message m_ER0002 = new Message(
            'R',
            'R',
            MessageType.ERROR,
            2,
            "Duplicate registration of type {0} does not match previous registration: previous type {1}, new type {2}");
    static Message m_WR0003 = new Message('C', 'R', MessageType.WARN, 3,
            "Duplicate registration of singleton {0} ignored");
    static Message m_ER0004 = new Message(
            'R',
            'R',
            MessageType.ERROR,
            4,
            "Duplicate registration of singleton {0} does not match previous registration: previous object {1}, new object {2}");
    static Message m_ER0005 = new Message('C', 'R', MessageType.ERROR, 5,
            "Error occurred when attempting to load class {0}");
    static Message m_ER0006 = new Message('C', 'R', MessageType.ERROR, 6,
            "Unknown typecode {0}");
    static Message m_ER0007 = new Message('C', 'R', MessageType.ERROR, 7,
            "Error occurred when attempting to create new instance of type {0} class {1}");

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
    // private final HashMap<Short, ObjectDefinition> typeRegistry = new
    // HashMap<Short, ObjectDefinition>();
    TypeRegistry typeRegistry = new TypeRegistry();

    public ObjectRegistryImpl(Platform platform, Properties properties) {
        this.platform = platform;
        PlatformObjects po = platform
                .getPlatformObjects(ObjectRegistry.LOGGER_NAME);
        log = po.getLogger();
        exceptionHandler = po.getExceptionHandler();
    }

    public synchronized final void registerObjectFactory(int tc,
            ObjectFactory objectFactory) {
        assert tc < Short.MAX_VALUE && tc > 0;
        String classname = objectFactory.getType().getName();
        if (log.isDebugEnabled()) {
            log.debug(getClass(), "register",
                    "SIMPLEDBM-DEBUG: Registering typecode " + tc
                            + " for class " + classname);
        }
        short typecode = (short) tc;
        ObjectDefinition od = typeRegistry.get(typecode);
        if (od != null) {
            if (!od.isSingleton() && od.getClassName().equals(classname)) {
                log.warn(getClass(), "register", new MessageInstance(
                        m_WR0001, typecode).toString());
                return;
            }
            exceptionHandler.errorThrow(getClass(), "register",
                    new ObjectCreationException(new MessageInstance(m_ER0002,
                            typecode, od.getClassName(), classname)));
        }
        typeRegistry.put(typecode, new FactoryObjectDefinition(typecode,
                objectFactory));
    }

    public synchronized final void registerSingleton(int tc, Object object) {
        assert tc < Short.MAX_VALUE && tc > 0;
        short typecode = (short) tc;
        if (log.isDebugEnabled()) {
            log.debug(getClass(), "register",
                    "SIMPLEDBM-DEBUG: Registering typecode " + tc
                            + " for Singleton " + object);
        }
        ObjectDefinition od = typeRegistry.get(typecode);
        if (od != null) {
            if (od.isSingleton() && od.getInstance() == object) {
                log.warn(getClass(), "register", new MessageInstance(
                        m_WR0003, typecode).toString());
                return;
            }
            exceptionHandler.errorThrow(getClass(), "register",
                    new ObjectCreationException(new MessageInstance(m_ER0004,
                            typecode, od.getInstance(), object)));
        }
        typeRegistry.put(typecode, new SingletonObjectDefinition(typecode,
                object));
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.simpledbm.common.registry.ObjectFactory#getInstance(int)
     */
    public Object getSingleton(int typecode) {
        ObjectDefinition od = typeRegistry.get((short) typecode);
        if (od == null) {
            exceptionHandler.errorThrow(getClass(),
                    "getInstance",
                    new ObjectCreationException.UnknownTypeException(
                            new MessageInstance(m_ER0006, typecode)));
        }
        return od.getInstance();
    }

    /**
     * Creates an instance of the specified type, and initializes it using the
     * supplied ByteBuffer; the class in question must have a constructor that
     * takes a ByteBuffer as the only parameter.
     * 
     * <p>
     * It is an error to invoke this on a singleton.
     * 
     * @param typecode The code for the type
     * @param buf The ByteBuffer to supply as argument to the object being
     *            created
     * @return Newly constructed object of the type
     */
    public Object getInstance(int typecode, ByteBuffer buf) {
        ObjectDefinition od = typeRegistry.get((short) typecode);
        if (od == null) {
            exceptionHandler.errorThrow(getClass(),
                    "getInstance",
                    new ObjectCreationException.UnknownTypeException(
                            new MessageInstance(m_ER0006, typecode)));
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
     * Holds the definition of a type, either its class or if it is a singleton,
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
            throw new UnsupportedOperationException();
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
