package com.sunnysuperman.repository;

public class RepositoryException extends RuntimeException {

	public RepositoryException() {
	}

	public RepositoryException(String msg) {
		super(msg);
	}

	public RepositoryException(Exception ex) {
		super(ex);
	}

	public RepositoryException(String msg, Exception ex) {
		super(msg, ex);
	}
}
