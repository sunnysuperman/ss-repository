package com.sunnysuperman.repository.db;

import java.util.Map;

public class SerializedRow {
	private String tableName;
	private Map<String, Object> data;
	private Map<String, Object> upsertData;
	private String[] idColumns;
	private Object[] idValues;
	private boolean idGeneration;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public Map<String, Object> getData() {
		return data;
	}

	public void setData(Map<String, Object> data) {
		this.data = data;
	}

	public Map<String, Object> getUpsertData() {
		return upsertData;
	}

	public void setUpsertData(Map<String, Object> upsertData) {
		this.upsertData = upsertData;
	}

	public String[] getIdColumns() {
		return idColumns;
	}

	public void setIdColumns(String[] idColumns) {
		this.idColumns = idColumns;
	}

	public Object[] getIdValues() {
		return idValues;
	}

	public void setIdValues(Object[] idValues) {
		this.idValues = idValues;
	}

	public boolean isIdGeneration() {
		return idGeneration;
	}

	public void setIdGeneration(boolean idGeneration) {
		this.idGeneration = idGeneration;
	}

}
