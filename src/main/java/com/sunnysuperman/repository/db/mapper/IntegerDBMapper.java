package com.sunnysuperman.repository.db.mapper;

import java.util.Map;

import com.sunnysuperman.commons.util.FormatUtil;

public class IntegerDBMapper implements DBMapper<Integer> {
    private IntegerDBMapper() {
    }

    @Override
    public Integer map(Map<String, Object> doc) {
        if (doc.isEmpty()) {
            return null;
        }
        return FormatUtil.parseInteger(doc.values().iterator().next());
    }

    private static final IntegerDBMapper INSTANCE = new IntegerDBMapper();

    public static final IntegerDBMapper getInstance() {
        return INSTANCE;
    }

}
