package com.sunnysuperman.repository.db;

import java.util.Map;

public class MapDBRowMapper implements DBRowMapper<Map<String, Object>> {
    private MapDBRowMapper() {

    }

    @Override
    public Map<String, Object> map(Map<String, Object> row) {
        return row;
    }

    private static final MapDBRowMapper INSTANCE = new MapDBRowMapper();

    public static MapDBRowMapper getInstance() {
        return INSTANCE;
    }

}
