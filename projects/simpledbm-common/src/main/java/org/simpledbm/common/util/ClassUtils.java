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
package org.simpledbm.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;

/**
 * Various Class Loader utilities.
 * 
 * @author Dibyendu Majumdar
 * @since 14.Jan.2005
 */
public final class ClassUtils {

    public ClassUtils() {
    }

    /**
     * Get the ClassLoader to use. We always use the current Thread's Context
     * ClassLoader. Assumption is that all threads within the application share
     * the same ClassLoader.
     */
    private ClassLoader getClassLoader() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        return cl;
    }

    /**
     * A wrapper for Class.forName() so that we can change the behaviour
     * globally without changing the rest of the code.
     * 
     * @param name Name of the class to be loaded
     */
    public Class<?> forName(String name) throws ClassNotFoundException {
        ClassLoader cl = getClassLoader();
        Class<?> clazz = null;
        clazz = Class.forName(name, true, cl);
        return clazz;
    }

    /**
     * Load a properties file from the classpath.
     * 
     * @param name Name of the properties file
     */
    public InputStream getResourceAsStream(String name) throws IOException {
        ClassLoader cl = getClassLoader();
        InputStream is = null;
        is = cl.getResourceAsStream(name);
        if (is == null) {
            throw new IOException("Unable to load resource" + name);
        }
        return is;
    }

    /**
     * Load a properties file from the classpath.
     * 
     * @param name Name of the properties file
     */
    public Properties getResourceAsProperties(String name) throws IOException {
        ClassLoader cl = getClassLoader();
        InputStream is = null;
        is = cl.getResourceAsStream(name);
        if (is == null) {
            throw new IOException("Unable to load resource " + name);
        }
        Properties props = new Properties();
        try {
            props.load(is);
        } finally {
            is.close();
        }
        return props;
    }

    /**
     * Helper for invoking an instance method that takes a single parameter.
     * This method also handles parameters of primitive type.
     * 
     * @param cl The class that the instance belongs to
     * @param instance The object on which we will invoke the method
     * @param methodName The method name
     * @param param The parameter
     */
    public Object invokeMethod(Class<?> cl, Object instance, String methodName,
            Object param) throws Throwable {
        Class<? extends Object> paramClass;
        if (param instanceof Integer)
            paramClass = Integer.TYPE;
        else if (param instanceof Long)
            paramClass = Long.TYPE;
        else if (param instanceof Short)
            paramClass = Short.TYPE;
        else if (param instanceof Boolean)
            paramClass = Boolean.TYPE;
        else if (param instanceof Double)
            paramClass = Double.TYPE;
        else if (param instanceof Float)
            paramClass = Float.TYPE;
        else if (param instanceof Character)
            paramClass = Character.TYPE;
        else if (param instanceof Byte)
            paramClass = Byte.TYPE;
        else
            paramClass = param.getClass();
        Method method = cl.getMethod(methodName, new Class[] { paramClass });
        try {
            return method.invoke(instance, new Object[] { param });
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    /**
     * Helper for invoking a static method that takes one parameter.
     * 
     * @param cl The class that implements the static method
     * @param methodName The method name
     * @param param A parameter
     * @param paramClass Class of the parameter
     */
    public Object invokeStaticMethod(Class<?> cl, String methodName,
            Object param, Class<?> paramClass) throws Throwable {
        Method method = cl.getMethod(methodName, new Class[] { paramClass });
        try {
            return method.invoke(null, new Object[] { param });
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    /**
     * Helper for invoking a constructor with one parameter.
     * 
     * @param className Class of which an instance is to be allocated
     * @param param Parameter
     * @param paramClass Type of the parameter
     */
    public Object createObject(String className, Object param,
            Class<?> paramClass) throws ClassNotFoundException,
            SecurityException, NoSuchMethodException, IllegalArgumentException,
            InstantiationException, IllegalAccessException,
            InvocationTargetException {
        Class<?> clazzImpl = forName(className);
        Constructor<?> ctor = clazzImpl
                .getConstructor(new Class[] { paramClass });
        Object instance = ctor.newInstance(new Object[] { param });
        return instance;
    }
}
