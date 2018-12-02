package com.sunnysuperman.repository.db.mapper;

import java.util.Map;

import com.sunnysuperman.commons.bean.ParseBeanOptions;
import com.sunnysuperman.repository.serialize.SerializeBean;
import com.sunnysuperman.repository.serialize.Serializer;

public class BeanDBMapper<T> implements DBMapper<T> {
    private Class<T> clazz;
    private ParseBeanOptions options;

    public BeanDBMapper(Class<T> clazz, ParseBeanOptions options) {
        super();
        this.clazz = clazz;
        this.options = options;
        if (clazz.getAnnotation(SerializeBean.class) == null) {
            throw new RuntimeException("Class " + clazz + " is not annotated with SerializeBean");
        }
    }

    public BeanDBMapper(Class<T> clazz) {
        this(clazz, null);
    }

    @Override
    public T map(Map<String, Object> row) {
        try {
            return Serializer.deserialize(row, clazz, options);
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
