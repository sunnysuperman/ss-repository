package com.sunnysuperman.repository;

import java.util.Map;

import com.sunnysuperman.repository.db.DefaultFieldConverter;

public interface DeserializeContext {

	Map<String, Object> getColumns();

	Object getColumn(String name);

	DefaultFieldConverter getDefaultFieldConverter();

}
