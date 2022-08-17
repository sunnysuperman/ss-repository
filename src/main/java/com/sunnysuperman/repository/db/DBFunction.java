package com.sunnysuperman.repository.db;

public class DBFunction {
	private String function;
	private Object[] params;

	public DBFunction(String function, Object[] params) {
		super();
		this.function = function;
		this.params = params;
	}

	public String getFunction() {
		return function;
	}

	public Object[] getParams() {
		return params;
	}

}
