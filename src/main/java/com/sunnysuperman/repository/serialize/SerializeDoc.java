package com.sunnysuperman.repository.serialize;

import java.util.Map;

public class SerializeDoc {
    private Map<String, Object> doc;
    private String[] idColumns;
    private Object[] idValues;
    private Class<? extends Number> idIncrementClass;
    private boolean upsert;

    public Map<String, Object> getDoc() {
        return doc;
    }

    public void setDoc(Map<String, Object> doc) {
        this.doc = doc;
    }

    public String[] getIdColumns() {
        return idColumns;
    }

    public void setIdColumns(String[] idColumns) {
        this.idColumns = idColumns;
    }

    public Object[] getIdValues() {
        return idValues;
    }

    public void setIdValues(Object[] idValues) {
        this.idValues = idValues;
    }

    public Class<? extends Number> getIdIncrementClass() {
        return idIncrementClass;
    }

    public void setIdIncrementClass(Class<? extends Number> idIncrementClass) {
        this.idIncrementClass = idIncrementClass;
    }

    public boolean isUpsert() {
        return upsert;
    }

    public void setUpsert(boolean upsert) {
        this.upsert = upsert;
    }

}
