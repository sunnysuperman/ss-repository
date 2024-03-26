package com.sunnysuperman.repository.db.mapper;

import java.util.Map;

import com.sunnysuperman.repository.db.DefaultFieldConverter;
import com.sunnysuperman.repository.db.EntityManager;

public class EntityMapper<T> implements DBMapper<T> {
	Class<T> entityClass;
	DefaultFieldConverter defaultFieldConverter;

	public EntityMapper(Class<T> entityClass, DefaultFieldConverter defaultFieldConverter) {
		super();
		this.entityClass = entityClass;
		this.defaultFieldConverter = defaultFieldConverter;
	}

	@Override
	public T map(Map<String, Object> row) {
		return EntityManager.deserialize(row, entityClass, defaultFieldConverter);
	}

}
