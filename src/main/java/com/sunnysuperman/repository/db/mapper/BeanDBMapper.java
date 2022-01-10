package com.sunnysuperman.repository.db.mapper;

import java.util.Map;

import com.sunnysuperman.commons.bean.ParseBeanOptions;
import com.sunnysuperman.repository.serialize.SerializeBean;
import com.sunnysuperman.repository.serialize.Serializer;

public class BeanDBMapper<T> implements DBMapper<T> {
    private Class<T> entityClass;
    private ParseBeanOptions options;

    public BeanDBMapper(Class<T> entityClass, ParseBeanOptions options) {
        super();
        this.entityClass = entityClass;
        this.options = options;
        if (entityClass.getAnnotation(SerializeBean.class) == null) {
            throw new RuntimeException("Class " + entityClass + " is not annotated with SerializeBean");
        }
    }

    public BeanDBMapper(Class<T> clazz) {
        this(clazz, null);
    }

    public Class<T> getEntityClass() {
        return entityClass;
    }

    public ParseBeanOptions getOptions() {
        return options;
    }

    @Override
    public T map(Map<String, Object> row) {
        try {
            return Serializer.deserialize(row, entityClass, options);
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
