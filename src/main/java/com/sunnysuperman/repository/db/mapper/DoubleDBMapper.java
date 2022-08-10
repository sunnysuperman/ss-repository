package com.sunnysuperman.repository.db.mapper;

import java.util.Map;

import com.sunnysuperman.commons.util.FormatUtil;

public class DoubleDBMapper implements DBMapper<Double> {
	private DoubleDBMapper() {
	}

	@Override
	public Double map(Map<String, Object> doc) {
		if (doc.isEmpty()) {
			return null;
		}
		return FormatUtil.parseDouble(doc.values().iterator().next());
	}

	private static final DoubleDBMapper INSTANCE = new DoubleDBMapper();

	public static final DoubleDBMapper getInstance() {
		return INSTANCE;
	}

}
