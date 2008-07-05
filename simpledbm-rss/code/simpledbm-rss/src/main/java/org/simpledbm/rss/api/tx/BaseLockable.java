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
package org.simpledbm.rss.api.tx;

import org.simpledbm.rss.util.Dumpable;

/**
 * BaseLockable provides a convenient base class which transactional modules
 * can use to create their own {@link Lockable} implementations.
 * 
 * @author Dibyendu Majumdar
 * @since 15 Dec 2006
 */
public abstract class BaseLockable implements Lockable, Dumpable {

    /**
     * Locks are divided into separate namespaces. For instance,
     * container locks and tuple locks are in different namespaces.
     * Locks in different namespaces can never conflict with each other.
     */
    final byte namespace;

    protected BaseLockable(byte namespace) {
        this.namespace = namespace;
    }

    protected byte getNameSpace() {
        return namespace;
    }

    @Override
    public int hashCode() {
        final int PRIME = 31;
        int result = 1;
        result = PRIME * result + namespace;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final BaseLockable other = (BaseLockable) obj;
        if (namespace != other.namespace)
            return false;
        return true;
    }

    public StringBuilder appendTo(StringBuilder sb) {
        sb.append("ns=").append(namespace);
        return sb;
    }

}
