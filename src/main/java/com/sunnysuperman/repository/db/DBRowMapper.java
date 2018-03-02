package com.sunnysuperman.repository.db;

import java.util.Map;

public interface DBRowMapper<T> {

    T map(Map<String, Object> row);

}
