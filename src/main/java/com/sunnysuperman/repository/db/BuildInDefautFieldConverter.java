package com.sunnysuperman.repository.db;

import java.lang.reflect.Type;
import java.math.BigDecimal;

import com.sunnysuperman.commons.util.FormatUtil;

public class BuildInDefautFieldConverter implements DefaultFieldConverter {
	private static final BuildInDefautFieldConverter INSTANCE = new BuildInDefautFieldConverter();

	public static BuildInDefautFieldConverter getInstance() {
		return INSTANCE;
	}

	@Override
	public Object convertToColumn(Object fieldValue) {
		Class<?> type = fieldValue.getClass();
		// 常见类型
		if (isSimpleType(type)) {
			return fieldValue;
		}
		// 默认转换
		return convertToColumnByDefault(fieldValue);
	}

	protected Object convertToColumnByDefault(Object fieldValue) {
		return fieldValue;
	}

	@Override
	public Object convertToField(Object columnValue, Class<?> type, Type genericType) {
		// 常见类型
		if (type == String.class) {
			return FormatUtil.parseString(columnValue);
		}
		if (type == Integer.class || type == int.class) {
			return FormatUtil.parseInteger(columnValue);
		}
		if (type == Long.class || type == long.class) {
			return FormatUtil.parseLong(columnValue);
		}
		if (type == Boolean.class || type == boolean.class) {
			return FormatUtil.parseBoolean(columnValue);
		}
		if (type == Double.class || type == double.class) {
			return FormatUtil.parseDouble(columnValue);
		}
		if (type == Float.class || type == float.class) {
			return FormatUtil.parseFloat(columnValue);
		}
		if (type == Byte.class || type == byte.class) {
			return FormatUtil.parseByte(columnValue);
		}
		if (type == BigDecimal.class) {
			return FormatUtil.parseDecimal(columnValue);
		}
		if (type == Short.class || type == short.class) {
			return FormatUtil.parseShort(columnValue);
		}
		return convertToFieldByDefault(columnValue, type, genericType);
	}

	protected Object convertToFieldByDefault(Object columnValue, Class<?> type, Type genericType) {
		return columnValue;
	}

	protected boolean isSimpleType(Class<?> type) {
		if (type.isPrimitive()) {
			return true;
		}
		if (type == String.class || type == Long.class || type == Integer.class || type == Double.class
				|| type == Float.class || type == Boolean.class || type == Byte.class || type == Short.class
				|| type == Character.class) {
			return true;
		}
		// 二进制
		return type.isArray() && type.getComponentType().equals(byte.class);
	}

}
