package com.sunnysuperman.repository.db.mapper;

import java.util.Map;

public interface DBMapper<T> {

    T map(Map<String, Object> row);

}
