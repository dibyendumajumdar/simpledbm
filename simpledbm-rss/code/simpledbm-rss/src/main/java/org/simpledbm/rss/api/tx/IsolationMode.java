/**
 * 
 */
package org.simpledbm.rss.api.tx;

public enum IsolationMode {
	READ_COMMITTED,
	CURSOR_STABILITY,
	REPEATABLE_READ
}