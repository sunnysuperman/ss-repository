package com.sunnysuperman.repository.db;

import java.util.Map;

import com.sunnysuperman.repository.DeserializeContext;

public class DBDeserializeContext implements DeserializeContext {
	Map<String, Object> doc;
	DefaultFieldConverter defaultFieldConverter;

	public DBDeserializeContext(Map<String, Object> doc, DefaultFieldConverter defaultFieldConverter) {
		super();
		this.doc = doc;
		this.defaultFieldConverter = defaultFieldConverter;
	}

	@Override
	public Map<String, Object> getColumns() {
		return doc;
	}

	@Override
	public Object getColumn(String name) {
		return doc.get(name);
	}

	@Override
	public DefaultFieldConverter getDefaultFieldConverter() {
		return defaultFieldConverter;
	}

}
