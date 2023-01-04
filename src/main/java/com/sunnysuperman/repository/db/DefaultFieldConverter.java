package com.sunnysuperman.repository.db;

import java.lang.reflect.Type;

public interface DefaultFieldConverter {

	Object convertToColumn(Object fieldValue);

	Object convertToField(Object columnValue, Class<?> type, Type genericType);
}
