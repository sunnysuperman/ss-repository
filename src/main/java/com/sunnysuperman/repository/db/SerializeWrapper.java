package com.sunnysuperman.repository.db;

import java.util.Map;

public interface SerializeWrapper<T> {

    Map<String, Object> wrap(Map<String, Object> doc, T bean);

}
