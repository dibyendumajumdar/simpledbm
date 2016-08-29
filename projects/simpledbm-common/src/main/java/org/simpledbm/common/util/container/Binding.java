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

import org.simpledbm.common.api.exception.SimpleDBMException;
import org.simpledbm.common.util.mcat.Message;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.common.util.mcat.MessageType;
import org.simpledbm.common.util.ClassUtils;

final class Binding {

    static final ClassUtils classUtils = new ClassUtils();

    static final Message m_classNotFound = new Message('P', 'C',
            MessageType.ERROR, 1, "Class {0} not found");
    static final Message m_classNotCompatible = new Message('P', 'C',
            MessageType.ERROR, 2, "Class {0} does not implement interface {1}");

    final String interfaceName;
    final Class<?> classType;

    public Binding(String interfaceName, String className) {
        this.interfaceName = interfaceName;
        try {
            this.classType = classUtils.forName(className);
        } catch (ClassNotFoundException e) {
            throw new SimpleDBMException(new MessageInstance(m_classNotFound,
                    className), e);
        }
        if (!implementsInterface(this.classType, interfaceName)) {
            throw new SimpleDBMException(new MessageInstance(
                    m_classNotCompatible, className, interfaceName));
        }
    }

    public Binding(Class<?> interfaceType, Class<?> classType) {
        this.interfaceName = interfaceType.getName();
        this.classType = classType;
        if (!implementsInterface(classType, interfaceType.getName())) {
            throw new SimpleDBMException(new MessageInstance(
                    m_classNotCompatible, classType.getName(), interfaceName));
        }
    }

    private final boolean implementsInterface(Class<?> klass,
            String interfaceName) {
        Class<?>[] ifaces = klass.getInterfaces();
        for (Class<?> iface : ifaces) {
            if (iface.getName().equals(interfaceName)) {
                return true;
            }
        }
        return false;
    }
}
