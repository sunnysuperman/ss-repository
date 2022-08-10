package com.sunnysuperman.repository.db.mapper;

import java.util.Map;

import com.sunnysuperman.commons.util.FormatUtil;

public class ByteDBMapper implements DBMapper<Byte> {
	private ByteDBMapper() {
	}

	@Override
	public Byte map(Map<String, Object> doc) {
		if (doc.isEmpty()) {
			return null;
		}
		return FormatUtil.parseByte(doc.values().iterator().next());
	}

	private static final ByteDBMapper INSTANCE = new ByteDBMapper();

	public static final ByteDBMapper getInstance() {
		return INSTANCE;
	}

}
