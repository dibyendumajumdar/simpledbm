package org.simpledbm.typesystem.api;

import org.simpledbm.common.api.exception.SimpleDBMException;


public class TypeException extends SimpleDBMException {

	private static final long serialVersionUID = 1L;

	public TypeException() {
	}

	public TypeException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

	public TypeException(String arg0) {
		super(arg0);
	}

	public TypeException(Throwable arg0) {
		super(arg0);
	}

}
