package com.sunnysuperman.repository.db.mapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.sunnysuperman.commons.bean.Bean;
import com.sunnysuperman.commons.bean.ParseBeanOptions;
import com.sunnysuperman.commons.util.StringUtil;

public class SimpleBeanDBRowMapper<T> implements DBRowMapper<T> {
    private Class<T> clazz;
    private ParseBeanOptions options;

    public SimpleBeanDBRowMapper(Class<T> clazz) {
        super();
        this.clazz = clazz;
    }

    public SimpleBeanDBRowMapper(Class<T> clazz, ParseBeanOptions options) {
        super();
        this.clazz = clazz;
        this.options = options;
    }

    @Override
    public T map(Map<String, Object> row) {
        Map<String, Object> doc = new HashMap<>();
        for (Entry<String, Object> entry : row.entrySet()) {
            doc.put(StringUtil.underline2camel(entry.getKey()), entry.getValue());
        }
        T bean = newInstance(doc);
        beforeParseBean(doc, bean);
        Bean.fromMap(doc, bean, options);
        afterParseBean(doc, bean);
        return bean;
    }

    protected T newInstance(Map<String, Object> doc) {
        T bean;
        try {
            bean = clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        return bean;
    }

    protected void beforeParseBean(Map<String, Object> doc, T bean) {

    }

    protected void afterParseBean(Map<String, Object> doc, T bean) {

    }

}
