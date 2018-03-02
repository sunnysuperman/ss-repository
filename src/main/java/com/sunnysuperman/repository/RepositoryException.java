package com.sunnysuperman.repository;

public class RepositoryException extends Exception {
    private static final long serialVersionUID = 1970461509744960226L;

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
