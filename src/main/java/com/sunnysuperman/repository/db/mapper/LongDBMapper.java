package com.sunnysuperman.repository.db.mapper;

import java.util.Map;

import com.sunnysuperman.commons.util.FormatUtil;

public class LongDBMapper implements DBMapper<Long> {
	private LongDBMapper() {
	}

	@Override
	public Long map(Map<String, Object> doc) {
		if (doc.isEmpty()) {
			return null;
		}
		return FormatUtil.parseLong(doc.values().iterator().next());
	}

	private static final LongDBMapper INSTANCE = new LongDBMapper();

	public static final LongDBMapper getInstance() {
		return INSTANCE;
	}

}
