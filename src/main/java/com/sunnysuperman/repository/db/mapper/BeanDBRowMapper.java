package com.sunnysuperman.repository.db.mapper;

import java.util.Map;

import com.sunnysuperman.repository.serialize.SerializeBeanUtils;

public class BeanDBRowMapper<T> implements DBRowMapper<T> {
    private Class<T> clazz;

    public BeanDBRowMapper(Class<T> clazz) {
        super();
        this.clazz = clazz;
    }

    @Override
    public T map(Map<String, Object> row) {
        try {
            return SerializeBeanUtils.deserialize(row, clazz);
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
