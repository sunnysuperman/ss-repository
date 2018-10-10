package com.sunnysuperman.repository.db.mapper;

import java.util.Map;

import com.sunnysuperman.commons.bean.ParseBeanOptions;
import com.sunnysuperman.repository.serialize.SerializeBean;
import com.sunnysuperman.repository.serialize.SerializeManager;

public class BeanDBRowMapper<T> implements DBRowMapper<T> {
    private Class<T> clazz;
    private ParseBeanOptions options;

    public BeanDBRowMapper(Class<T> clazz, ParseBeanOptions options) {
        super();
        this.clazz = clazz;
        this.options = options;
        if (clazz.getAnnotation(SerializeBean.class) == null) {
            throw new RuntimeException("Class " + clazz + " is not annotated with SerializeBean");
        }
    }

    public BeanDBRowMapper(Class<T> clazz) {
        this(clazz, null);
    }

    @Override
    public T map(Map<String, Object> row) {
        try {
            return SerializeManager.deserialize(row, clazz, options);
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
