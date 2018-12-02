package com.sunnysuperman.repository.serialize;

import java.util.Map;

public class SerializeDoc {
    private Map<String, Object> doc;
    private String[] idColumns;
    private Object[] idValues;
    private Class<? extends Number> idIncrementClass;
    private Map<String, Object> upsertDoc;
    private String tableName;

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

    public Map<String, Object> getUpsertDoc() {
        return upsertDoc;
    }

    public void setUpsertDoc(Map<String, Object> upsertDoc) {
        this.upsertDoc = upsertDoc;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

}
