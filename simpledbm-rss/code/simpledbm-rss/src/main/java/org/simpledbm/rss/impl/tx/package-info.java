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

/**
 * Default implementation of the SimpleDBM Transaction Manager as specified in {@link org.simpledbm.rss.api.tx}.
 * The implementation is closely modeled upon the ARIES algorithm, described in the paper by C.Mohan and others 
 * entitled:<cite>ARIES: A Transaction Recovery Method Supporting Fine-Granularity Locking and Partial Rollbacks 
 * Using Write-Ahead Logging</cite>.
 */
package org.simpledbm.rss.impl.tx;

