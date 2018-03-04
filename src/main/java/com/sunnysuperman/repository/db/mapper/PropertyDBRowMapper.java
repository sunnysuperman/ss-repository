package com.sunnysuperman.repository.db.mapper;

import java.util.Map;

public class PropertyDBRowMapper implements DBRowMapper<Object> {
    private PropertyDBRowMapper() {
    }

    @Override
    public Object map(Map<String, Object> doc) {
        if (doc.isEmpty()) {
            return null;
        }
        return doc.values().iterator().next();
    }

    private static final PropertyDBRowMapper INSTANCE = new PropertyDBRowMapper();

    public static final PropertyDBRowMapper getInstance() {
        return INSTANCE;
    }

}
