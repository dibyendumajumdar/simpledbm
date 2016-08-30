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
package org.simpledbm.typesystem.api;

import java.text.DateFormat;
import java.util.TimeZone;

import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.util.Dumpable;

/**
 * Provides type information.
 * 
 * @author Dibyendu Majumdar
 */
public interface TypeDescriptor extends Storable, Dumpable {

    static final int TYPE_INTEGER = 1;
    static final int TYPE_VARCHAR = 2;
    static final int TYPE_NUMBER = 3;
    static final int TYPE_LONG_INTEGER = 4;
    static final int TYPE_DATETIME = 5;
    static final int TYPE_BINARY = 6;

    /**
     * Returns a unique integer code for the type. This is useful for
     * serializing the type.
     */
    int getTypeCode();

    /**
     * Returns the maximum length of any value of this type.
     */
    int getMaxLength();

    /**
     * Returns the scale for certain numeric types. Scale defines the number of
     * digits to the right of the decimal point.
     */
    int getScale();

    /**
     * Returns the timezone associated with the type - only applicable for date
     * and time types.
     */
    TimeZone getTimeZone();

    /**
     * Returns the date formatter object associated with the type - only
     * applicable for date and time types.
     */
    DateFormat getDateFormat();

}
