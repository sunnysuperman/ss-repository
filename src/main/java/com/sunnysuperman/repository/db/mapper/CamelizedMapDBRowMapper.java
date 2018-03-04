package com.sunnysuperman.repository.db.mapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.sunnysuperman.commons.util.StringUtil;

public class CamelizedMapDBRowMapper implements DBRowMapper<Map<String, Object>> {
    private CamelizedMapDBRowMapper() {
    }

    @Override
    public Map<String, Object> map(Map<String, Object> row) {
        Map<String, Object> doc = new HashMap<>();
        for (Entry<String, Object> entry : row.entrySet()) {
            doc.put(StringUtil.underline2camel(entry.getKey()), entry.getValue());
        }
        return doc;
    }

    private static final CamelizedMapDBRowMapper INSTANCE = new CamelizedMapDBRowMapper();

    public static CamelizedMapDBRowMapper getInstance() {
        return INSTANCE;
    }

}
