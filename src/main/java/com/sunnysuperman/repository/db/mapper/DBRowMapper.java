package com.sunnysuperman.repository.db.mapper;

import java.util.Map;

public interface DBRowMapper<T> {

    T map(Map<String, Object> row);

}
