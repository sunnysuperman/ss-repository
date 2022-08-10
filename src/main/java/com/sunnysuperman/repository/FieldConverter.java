package com.sunnysuperman.repository;

public interface FieldConverter<T> {

	Object convertToColumn(T fieldValue);

	T convertToField(Object columnValue);
}
