package com.sunnysuperman.repository.db.mapper;

import java.util.Map;

import com.sunnysuperman.repository.serialize.SerializeBean;
import com.sunnysuperman.repository.serialize.SerializeManager;

public class BeanDBRowMapper<T> implements DBRowMapper<T> {
    private Class<T> clazz;

    public BeanDBRowMapper(Class<T> clazz) {
        super();
        this.clazz = clazz;
        if (clazz.getAnnotation(SerializeBean.class) == null) {
            throw new RuntimeException("Class " + clazz + " is not annotated with SerializeBean");
        }
    }

    @Override
    public T map(Map<String, Object> row) {
        try {
            return SerializeManager.deserialize(row, clazz);
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
