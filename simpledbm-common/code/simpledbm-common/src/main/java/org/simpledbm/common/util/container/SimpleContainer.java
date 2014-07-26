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
package org.simpledbm.common.util.container;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import org.simpledbm.common.api.exception.SimpleDBMException;
import org.simpledbm.common.util.mcat.Message;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.common.util.mcat.MessageType;
import org.simpledbm.common.util.ClassUtils;

/**
 * A Simple IOC Container that supports singletons only; auto wiring of
 * dependencies via constructor injection is supported. The properties supplied
 * to the container are available as an instance of
 * {@link org.simpledbm.common.util.container.ConfigProperties}. An
 * implementation that wants to access the properties should declare a
 * constructor parameter of above type. Example:
 * 
 * <pre>
 * class MyClass {
 *     public MyClass(
 *             org.simpledbm.common.util.container.ConfigProperties properties) {
 *     }
 * }
 * </pre>
 * 
 * @author dibyendumajumdar
 * 
 */
public final class SimpleContainer {

    static final Message m_constructFailed = new Message('P', 'C',
            MessageType.ERROR, 5,
            "Error occurred when creating an instance of {0}");
    static final Message m_constructorNotFound = new Message('P', 'C',
            MessageType.ERROR, 6,
            "A suitable constructor for class {0} was not found");


    final HashMap<String, Object> map = new HashMap<String, Object>();
    final ClassUtils classUtils = new ClassUtils();

    private Object constructObject(Binding binding) {
        Class<?> klass = binding.classType;
        Constructor<?> ctors[] = klass.getConstructors();
        int bestMatch = -1;
        int bestMatchedParameters = 0;
        for (int i = 0; i < ctors.length; i++) {
            Constructor<?> ctor = ctors[i];
            int matchedParameters = 0;
            Class<?> parmTypes[] = ctor.getParameterTypes();
            if (parmTypes.length == 0 && bestMatch == -1) {
                bestMatch = i;
                continue;
            }
            for (int p = 0; p < parmTypes.length; p++) {
                Class<?> parmType = parmTypes[p];
                if (!parmType.isInterface()) {
                    // all parameters must be interfaces
                    break;
                }
                if (map.get(parmType.getName()) != null) {
                    matchedParameters++;
                    break;
                }
            }
            if (matchedParameters > bestMatchedParameters) {
                bestMatchedParameters = matchedParameters;
                bestMatch = i;
            }
        }
        if (bestMatch == -1) {
            throw new SimpleDBMException(new MessageInstance(
                    m_constructorNotFound, klass.getName()));
        }
        Constructor<?> ctor = ctors[bestMatch];
        Class<?> parmTypes[] = ctor.getParameterTypes();
        Object parms[] = new Object[parmTypes.length];
        for (int p = 0; p < parmTypes.length; p++) {
            Class<?> parmType = parmTypes[p];
            // is an object registered with this interface
            parms[p] = map.get(parmType.getName());
        }
        Object obj = null;
        try {
            obj = ctor.newInstance(parms);
        } catch (IllegalArgumentException e) {
            throw new SimpleDBMException(new MessageInstance(m_constructFailed,
                    binding.classType.getName()), e);
        } catch (InstantiationException e) {
            throw new SimpleDBMException(new MessageInstance(m_constructFailed,
                    binding.classType.getName()), e);
        } catch (IllegalAccessException e) {
            throw new SimpleDBMException(new MessageInstance(m_constructFailed,
                    binding.classType.getName()), e);
        } catch (InvocationTargetException e) {
            throw new SimpleDBMException(new MessageInstance(m_constructFailed,
                    binding.classType.getName()), e);
        }
        map.put(binding.interfaceName, obj);
        return obj;
    }

    public SimpleContainer(Properties properties, ArrayList<Binding> list) {
        ConfigPropertiesImpl configPropertiesImpl = new ConfigPropertiesImpl(
                properties);
        map.put(ConfigProperties.class.getName(), configPropertiesImpl);
        for (Binding r : list) {
            constructObject(r);
        }
    }

    public final <T> T get(Class<T> c) {
        Object obj = map.get(c.getName());
        if (obj != null) {
            return c.cast(obj);
        }
        return null;
    }
}
