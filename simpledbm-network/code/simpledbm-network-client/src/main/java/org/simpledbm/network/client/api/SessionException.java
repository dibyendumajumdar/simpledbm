package org.simpledbm.network.client.api;

public class SessionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public SessionException() {
    }

    public SessionException(String arg0) {
        super(arg0);
    }

    public SessionException(Throwable arg0) {
        super(arg0);
    }

    public SessionException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

}
