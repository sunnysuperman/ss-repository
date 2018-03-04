package com.sunnysuperman.repository.db.mapper;

import java.util.Map;

import com.sunnysuperman.commons.util.FormatUtil;
import com.sunnysuperman.repository.db.mapper.DBRowMapper;

public class StringDBRowMapper implements DBRowMapper<String> {

    private StringDBRowMapper() {
    }

    @Override
    public String map(Map<String, Object> doc) {
        if (doc.isEmpty()) {
            return null;
        }
        return FormatUtil.parseString(doc.values().iterator().next());
    }

    private static final StringDBRowMapper INSTANCE = new StringDBRowMapper();

    public static final StringDBRowMapper getInstance() {
        return INSTANCE;
    }

}
