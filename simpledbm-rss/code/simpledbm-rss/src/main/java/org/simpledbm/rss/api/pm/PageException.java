/*
 * Created on: 25-Nov-2005
 * Author: Dibyendu Majumdar
 */
package org.simpledbm.rss.api.pm;

public class PageException extends Exception {

    private static final long serialVersionUID = -5924553754446182755L;

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

    public static final class StorageException extends PageException {

        private static final long serialVersionUID = 2678438490352669470L;

        public StorageException() {
            super();
            // TODO Auto-generated constructor stub
        }

        public StorageException(String arg0, Throwable arg1) {
            super(arg0, arg1);
            // TODO Auto-generated constructor stub
        }

        public StorageException(String arg0) {
            super(arg0);
            // TODO Auto-generated constructor stub
        }

        public StorageException(Throwable arg0) {
            super(arg0);
            // TODO Auto-generated constructor stub
        }
        
        
        
    }
    
}
