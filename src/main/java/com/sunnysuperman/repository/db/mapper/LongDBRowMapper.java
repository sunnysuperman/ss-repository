package com.sunnysuperman.repository.db.mapper;

import java.util.Map;

import com.sunnysuperman.commons.util.FormatUtil;
import com.sunnysuperman.repository.db.mapper.DBRowMapper;

public class LongDBRowMapper implements DBRowMapper<Long> {
    private LongDBRowMapper() {
    }

    @Override
    public Long map(Map<String, Object> doc) {
        if (doc.isEmpty()) {
            return null;
        }
        return FormatUtil.parseLong(doc.values().iterator().next());
    }

    private static final LongDBRowMapper INSTANCE = new LongDBRowMapper();

    public static final LongDBRowMapper getInstance() {
        return INSTANCE;
    }

}
