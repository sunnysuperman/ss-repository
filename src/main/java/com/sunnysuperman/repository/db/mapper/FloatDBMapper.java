package com.sunnysuperman.repository.db.mapper;

import java.util.Map;

import com.sunnysuperman.commons.util.FormatUtil;

public class FloatDBMapper implements DBMapper<Double> {
	private FloatDBMapper() {
	}

	@Override
	public Double map(Map<String, Object> doc) {
		if (doc.isEmpty()) {
			return null;
		}
		return FormatUtil.parseDouble(doc.values().iterator().next());
	}

	private static final FloatDBMapper INSTANCE = new FloatDBMapper();

	public static final FloatDBMapper getInstance() {
		return INSTANCE;
	}

}
