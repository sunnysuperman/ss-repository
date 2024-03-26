package com.sunnysuperman.repository.exception;

import com.sunnysuperman.repository.RepositoryException;

@SuppressWarnings("serial")
public class StaleEntityRepositoryException extends RepositoryException {

	public StaleEntityRepositoryException() {
	}

	public StaleEntityRepositoryException(String msg) {
		super(msg);
	}

}
