package com.sunnysuperman.repository.db.mapper;

import java.util.Map;

import com.sunnysuperman.commons.util.FormatUtil;
import com.sunnysuperman.repository.db.mapper.DBMapper;

public class StringDBMapper implements DBMapper<String> {

    private StringDBMapper() {
    }

    @Override
    public String map(Map<String, Object> doc) {
        if (doc.isEmpty()) {
            return null;
        }
        return FormatUtil.parseString(doc.values().iterator().next());
    }

    private static final StringDBMapper INSTANCE = new StringDBMapper();

    public static final StringDBMapper getInstance() {
        return INSTANCE;
    }

}
