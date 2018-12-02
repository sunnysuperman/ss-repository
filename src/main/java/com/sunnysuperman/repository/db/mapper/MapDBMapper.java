package com.sunnysuperman.repository.db.mapper;

import java.util.Map;

public class MapDBMapper implements DBMapper<Map<String, Object>> {
    private MapDBMapper() {
    }

    @Override
    public Map<String, Object> map(Map<String, Object> row) {
        return row;
    }

    private static final MapDBMapper INSTANCE = new MapDBMapper();

    public static MapDBMapper getInstance() {
        return INSTANCE;
    }

}
