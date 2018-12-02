package com.sunnysuperman.repository.db.mapper;

import java.util.Map;

public class ObjectDBMapper implements DBMapper<Object> {
    private ObjectDBMapper() {
    }

    @Override
    public Object map(Map<String, Object> doc) {
        if (doc.isEmpty()) {
            return null;
        }
        return doc.values().iterator().next();
    }

    private static final ObjectDBMapper INSTANCE = new ObjectDBMapper();

    public static final ObjectDBMapper getInstance() {
        return INSTANCE;
    }

}
