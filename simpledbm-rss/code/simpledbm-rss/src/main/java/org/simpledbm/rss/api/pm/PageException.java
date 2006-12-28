/*
 * Created on: 25-Nov-2005
 * Author: Dibyendu Majumdar
 */
package org.simpledbm.rss.api.pm;

import org.simpledbm.rss.api.exception.RSSException;

public class PageException extends RSSException {

    private static final long serialVersionUID = 1L;

    public PageException() {
        super();
    }

    public PageException(String arg0) {
        super(arg0);
    }

    public PageException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    public PageException(Throwable arg0) {
        super(arg0);
    }

}
