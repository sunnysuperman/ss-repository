package com.sunnysuperman.repository.db.mapper;

import java.util.Map;

import com.sunnysuperman.commons.util.FormatUtil;

public class IntegerDBRowMapper implements DBRowMapper<Integer> {
    private IntegerDBRowMapper() {
    }

    @Override
    public Integer map(Map<String, Object> doc) {
        if (doc.isEmpty()) {
            return null;
        }
        return FormatUtil.parseInteger(doc.values().iterator().next());
    }

    private static final IntegerDBRowMapper INSTANCE = new IntegerDBRowMapper();

    public static final IntegerDBRowMapper getInstance() {
        return INSTANCE;
    }

}
