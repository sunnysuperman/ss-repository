package com.sunnysuperman.repository.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;

import com.sunnysuperman.commons.model.Pagination;
import com.sunnysuperman.commons.model.PullPagination;
import com.sunnysuperman.commons.util.FormatUtil;
import com.sunnysuperman.commons.util.JSONUtil;
import com.sunnysuperman.repository.InsertUpdate;
import com.sunnysuperman.repository.RepositoryException;
import com.sunnysuperman.repository.SaveResult;
import com.sunnysuperman.repository.db.mapper.DBRowMapper;
import com.sunnysuperman.repository.serialize.SerializeBean;
import com.sunnysuperman.repository.serialize.SerializeDoc;
import com.sunnysuperman.repository.serialize.SerializeManager;

public abstract class DBRepository {

    protected abstract JdbcTemplate getJdbcTemplate();

    private static boolean isSimpleType(Class<?> type) {
        return (type.isPrimitive() && type != void.class) || type == Double.class || type == Float.class
                || type == Long.class || type == Integer.class || type == Short.class || type == Character.class
                || type == Byte.class || type == Boolean.class || type == String.class;
    }

    protected Object serializeObject(Object value) {
        if (value == null) {
            return null;
        }
        if (isSimpleType(value.getClass())) {
            return value;
        }
        if (value instanceof Date) {
            return ((Date) value).getTime();
        }
        if (value.getClass().isArray() && value.getClass().getComponentType().equals(byte.class)) {
            // byte array (should be blob type)
            return value;
        }
        return JSONUtil.toJSONString(value);
    }

    protected Object[] serializeParams(Object[] params) {
        if (params == null || params.length == 0) {
            return null;
        }
        for (int i = 0; i < params.length; i++) {
            params[i] = serializeObject(params[i]);
        }
        return params;
    }

    protected String convertColumnName(String columnName) {
        return columnName;
    }

    protected String getInsertDialect(Map<String, ?> kv, String tableName, List<Object> params, boolean ignore) {
        StringBuilder buf = new StringBuilder(ignore ? "insert ignore into " : "insert into ");
        buf.append(tableName);
        buf.append('(');

        StringBuilder buf2 = new StringBuilder();

        int i = -1;
        for (Entry<String, ?> entry : kv.entrySet()) {
            i++;
            if (i > 0) {
                buf.append(',');
                buf2.append(',');
            }
            buf.append(convertColumnName(entry.getKey()));
            Object fieldValue = entry.getValue();
            if (fieldValue != null && fieldValue instanceof DBFunction) {
                DBFunction cf = (DBFunction) fieldValue;
                buf2.append(cf.getFunction());
                if (cf.getParams() != null) {
                    for (Object param : cf.getParams()) {
                        params.add(param);
                    }
                }
            } else {
                buf2.append('?');
                params.add(fieldValue);
            }
        }

        buf.append(") values(");
        buf.append(buf2);
        buf.append(')');
        return buf.toString();
    }

    protected String getPaginationDialect(String sql, int offset, int limit) {
        if (limit <= 0) {
            return sql;
        }
        return new StringBuilder(sql).append(" limit ").append(offset).append(",").append(limit).toString();
    }

    protected String getCountDialect(String sql) {
        int index1 = sql.indexOf("from");
        if (index1 <= 0) {
            throw new RuntimeException("Bad sql: " + sql);
        }
        int index2 = sql.indexOf(" order by", index1);
        if (index2 < 0) {
            index2 = sql.length();
        }
        StringBuilder buf = new StringBuilder("select count(*) ").append(sql.substring(index1, index2));
        return buf.toString();
    }

    public int execute(String sql, Object[] params) {
        return getJdbcTemplate().update(sql, serializeParams(params));
    }

    public int[] batchExecute(String sql, List<Object[]> batchParams) {
        return getJdbcTemplate().batchUpdate(sql, batchParams);
    }

    public boolean insertDoc(String tableName, Map<String, ?> doc, boolean ignore) {
        List<Object> params = new ArrayList<Object>(doc.size());
        String sql = getInsertDialect(doc, tableName, params, ignore);
        return execute(sql, params.toArray(new Object[params.size()])) > 0;
    }

    public boolean insertDoc(String tableName, Map<String, ?> doc) {
        return insertDoc(tableName, doc, false);
    }

    @SuppressWarnings("unchecked")
    public <T extends Number> T insertDoc(String tableName, Map<String, ?> doc, boolean ignore,
            Class<T> generatedKeyClass) {
        List<Object> params = new ArrayList<Object>(doc.size());
        String sql = getInsertDialect(doc, tableName, params, ignore);
        Object[] paramsArray = serializeParams(params.toArray(new Object[params.size()]));
        GeneratedKeyHolder holder = new GeneratedKeyHolder();
        boolean inserted = getJdbcTemplate().update(new CreateWithGeneratedKeyStatement(sql, paramsArray), holder) > 0;
        if (!inserted) {
            if (ignore) {
                return null;
            }
            // should not happended
            throw new RepositoryException("Insert error");
        }
        Number key;
        if (generatedKeyClass == Long.class) {
            key = FormatUtil.parseLong(holder.getKey());
        } else if (generatedKeyClass == Integer.class) {
            key = FormatUtil.parseInteger(holder.getKey());
        } else {
            throw new RepositoryException("Unknown generated key class: " + generatedKeyClass);
        }
        return (T) key;
    }

    public <T extends Number> T insertDoc(String tableName, Map<String, ?> doc, Class<T> generatedKeyClass) {
        return insertDoc(tableName, doc, false, generatedKeyClass);
    }

    public void insertDocs(String tableName, List<Map<String, Object>> docs) {
        Map<String, Object> testDoc = docs.get(0);
        String[] keys = new String[testDoc.size()];
        List<Object[]> paramsList = new ArrayList<>(docs.size());
        String sql;
        {
            int i = 0;
            StringBuilder buf = new StringBuilder("insert into ");
            StringBuilder buf2 = new StringBuilder();
            buf.append(tableName);
            buf.append('(');
            for (Entry<String, Object> entry : testDoc.entrySet()) {
                String key = entry.getKey();
                keys[i] = key;
                if (i > 0) {
                    buf.append(',');
                    buf2.append(',');
                }
                buf.append(convertColumnName(key));
                buf2.append('?');
                i++;
            }

            buf.append(") values(");
            buf.append(buf2);
            buf.append(')');
            sql = buf.toString();
        }
        for (Map<String, Object> doc : docs) {
            Object[] params = new Object[keys.length];
            for (int i = 0; i < keys.length; i++) {
                Object value = serializeObject(doc.get(keys[i]));
                params[i] = value;
            }
            paramsList.add(params);
        }
        getJdbcTemplate().batchUpdate(sql, paramsList);
    }

    public int updateDoc(String tableName, Map<String, ?> doc, String[] keys, Object[] values) {
        StringBuilder buf = new StringBuilder("update ");
        buf.append(tableName);
        buf.append(" set ");
        List<Object> params = new LinkedList<Object>();
        int offset = 0;
        for (Entry<String, ?> entry : doc.entrySet()) {
            String fieldKey = entry.getKey();
            Object fieldValue = entry.getValue();
            if (fieldKey.indexOf('$') == 0) {
                if (fieldKey.equals("$inc")) {
                    Map<?, ?> fieldValueAsMap = (Map<?, ?>) fieldValue;
                    for (Entry<?, ?> incEntry : fieldValueAsMap.entrySet()) {
                        String incKey = convertColumnName(incEntry.getKey().toString());
                        Number incValue = FormatUtil.parseNumber(incEntry.getValue());
                        if (offset > 0) {
                            buf.append(",");
                        }
                        offset++;
                        buf.append(incKey).append("=").append(incKey).append("+?");
                        params.add(incValue);
                    }
                }
                continue;
            }
            fieldKey = convertColumnName(fieldKey);
            if (offset > 0) {
                buf.append(",");
            }
            offset++;
            buf.append(fieldKey);
            if (fieldValue != null && fieldValue instanceof DBFunction) {
                // column=ST_GeometryFromText(?,4326), column=point(?,?), etc.
                DBFunction func = (DBFunction) fieldValue;
                buf.append("=").append(func.getFunction());
                if (func.getParams() != null) {
                    for (Object param : func.getParams()) {
                        params.add(param);
                    }
                }
            } else {
                buf.append("=?");
                params.add(fieldValue);
            }
        }
        buf.append(" where ");
        for (int i = 0; i < keys.length; i++) {
            if (i > 0) {
                buf.append(" and ");
            }
            String pk = keys[i];
            buf.append(convertColumnName(pk));
            buf.append("=?");
            params.add(values[i]);
        }
        Object[] paramsAsArray = params.toArray(new Object[params.size()]);
        return execute(buf.toString(), paramsAsArray);
    }

    public int updateDoc(String tableName, Map<String, ?> doc, String key, Object value) {
        return updateDoc(tableName, doc, new String[] { key }, new Object[] { value });
    }

    public InsertUpdate saveDoc(String tableName, Map<String, Object> doc, String primaryKey, boolean ignore) {
        Object primaryValue = doc.remove(primaryKey);
        if (primaryValue == null) {
            boolean inserted = insertDoc(tableName, doc, ignore);
            return inserted ? InsertUpdate.INSERT : null;
        }
        boolean updated = updateDoc(tableName, doc, primaryKey, primaryValue) > 0;
        if (updated) {
            return InsertUpdate.UPDATE;
        }
        doc.put(primaryKey, primaryValue);
        boolean inserted = insertDoc(tableName, doc, ignore);
        return inserted ? InsertUpdate.INSERT : null;
    }

    public <T> SaveResult save(T bean, Set<String> fields, InsertUpdate insertUpdate, SerializeDocWrapper<T> wrapper) {
        String tableName = bean.getClass().getAnnotation(SerializeBean.class).value();
        SerializeDoc sdoc;
        try {
            sdoc = SerializeManager.serialize(bean, fields, insertUpdate);
        } catch (Exception ex) {
            throw new RepositoryException(ex);
        }
        SaveResult result = new SaveResult();
        Map<String, Object> doc = sdoc.getDoc();
        if (wrapper != null) {
            doc = wrapper.wrap(doc, bean);
        }
        if (sdoc.getIdValues() != null) {
            boolean updated = updateDoc(tableName, doc, sdoc.getIdColumns(), sdoc.getIdValues()) > 0;
            if (!updated && sdoc.isUpsert()) {
                for (int i = 0; i < sdoc.getIdColumns().length; i++) {
                    doc.put(sdoc.getIdColumns()[i], sdoc.getIdValues()[i]);
                }
                boolean inserted = insertDoc(tableName, doc);
                result.setInserted(inserted);
            } else {
                result.setUpdated(updated);
            }
        } else {
            Class<? extends Number> idIncrementClass = sdoc.getIdIncrementClass();
            Number id;
            if (idIncrementClass != null) {
                id = insertDoc(tableName, doc, false, idIncrementClass);
                if (id == null) {
                    throw new RepositoryException("Failed to insert and generate id");
                }
                result.setInserted(true);
                result.setGeneratedId(id);
                SerializeManager.setIncrementId(bean, id);
            } else {
                boolean inserted = insertDoc(tableName, doc);
                result.setInserted(inserted);
            }
        }
        return result;
    }

    public SaveResult save(Object bean) {
        return save(bean, null, InsertUpdate.RUNTIME, null);
    }

    public <T> SaveResult save(T bean, SerializeDocWrapper<T> wrapper) {
        return save(bean, null, InsertUpdate.RUNTIME, wrapper);
    }

    public Object insert(Object bean) {
        return save(bean, null, InsertUpdate.INSERT, null).getGeneratedId();
    }

    public <T> Object insert(T bean, SerializeDocWrapper<T> wrapper) {
        return save(bean, null, InsertUpdate.INSERT, wrapper).getGeneratedId();
    }

    public boolean update(Object bean, Set<String> fields) {
        return save(bean, fields, InsertUpdate.UPDATE, null).isUpdated();
    }

    public <T> boolean update(T bean, Set<String> fields, SerializeDocWrapper<T> wrapper) {
        return save(bean, fields, InsertUpdate.UPDATE, wrapper).isUpdated();
    }

    public <T> T query(String sql, Object[] params, DBRowMapper<T> mapper) {
        List<Map<String, Object>> rawItems = getJdbcTemplate().queryForList(getPaginationDialect(sql, 0, 1), params);
        if (rawItems.isEmpty()) {
            return null;
        }
        Map<String, Object> rawItem = rawItems.get(0);
        return mapper.map(rawItem);
    }

    public <T> List<T> queryForList(String sql, Object[] params, int offset, int limit, DBRowMapper<T> mapper) {
        List<Map<String, Object>> rawItems = getJdbcTemplate().queryForList(getPaginationDialect(sql, offset, limit),
                params);
        if (rawItems.isEmpty()) {
            return Collections.emptyList();
        }
        List<T> items = new ArrayList<>(rawItems.size());
        for (Map<String, Object> rawItem : rawItems) {
            T item = mapper.map(rawItem);
            items.add(item);
        }
        return items;
    }

    public <T> Set<T> queryForSet(String sql, Object[] params, int offset, int limit, DBRowMapper<T> mapper) {
        List<Map<String, Object>> rawItems = getJdbcTemplate().queryForList(getPaginationDialect(sql, offset, limit),
                params);
        if (rawItems.isEmpty()) {
            return Collections.emptySet();
        }
        Set<T> items = new HashSet<>(rawItems.size());
        for (Map<String, Object> rawItem : rawItems) {
            T item = mapper.map(rawItem);
            items.add(item);
        }
        return items;
    }

    public <T> Pagination<T> queryForPagination(String sql, Object[] params, int offset, int limit,
            DBRowMapper<T> mapper) {
        List<T> items = queryForList(sql, params, offset, limit, mapper);
        int size = items.size();
        if (size == 0) {
            return Pagination.emptyInstance(limit);
        }
        if (offset != 0 || size == limit) {
            size = count(getCountDialect(sql), params);
        }
        return new Pagination<T>(items, size, offset, limit);
    }

    public <T> PullPagination<T> queryForPullPagination(String sql, Object[] params, String marker, int limit,
            DBRowMapper<T> mapper) {
        int offset = marker == null ? 0 : Integer.parseInt(marker);
        List<Map<String, Object>> rawItems = getJdbcTemplate()
                .queryForList(getPaginationDialect(sql, offset, limit + 1), params);
        if (rawItems.isEmpty()) {
            return PullPagination.emptyInstance();
        }
        boolean hasMore = rawItems.size() > limit;
        List<T> items = new ArrayList<>(Math.min(limit, rawItems.size()));
        for (Map<String, Object> rawItem : rawItems) {
            T item = mapper.map(rawItem);
            items.add(item);
            if (items.size() >= limit) {
                break;
            }
        }
        int newOffset = offset + limit;
        return PullPagination.newInstance(items, String.valueOf(newOffset), hasMore);
    }

    public <T> T queryByKey(String table, String selectKeys, String keyName, Object key, DBRowMapper<T> mapper) {
        StringBuilder sql = new StringBuilder("select ").append(selectKeys != null ? selectKeys : "*").append(" from ")
                .append(table).append(" where ").append(convertColumnName(keyName)).append("=?");
        return query(sql.toString(), new Object[] { key }, mapper);
    }

    public <T> List<T> queryByKeys(String table, String selectKeys, String keyName, Collection<?> keys,
            DBRowMapper<T> mapper) {
        StringBuilder sql = new StringBuilder("select ").append(selectKeys != null ? selectKeys : "*").append(" from ")
                .append(table).append(" where ").append(convertColumnName(keyName)).append(" in(");
        for (int i = 0; i < keys.size(); i++) {
            if (i > 0) {
                sql.append(",?");
            } else {
                sql.append('?');
            }
        }
        sql.append(')');
        return queryForList(sql.toString(), keys.toArray(new Object[keys.size()]), 0, 0, mapper);
    }

    public int count(String sql, Object[] params) {
        Integer val = getJdbcTemplate().queryForObject(sql, params, Integer.class);
        return FormatUtil.parseIntValue(val, 0);
    }

    public int removeByKey(String table, String keyName, Object key) {
        StringBuilder sql = new StringBuilder("delete from ").append(table).append(" where ")
                .append(convertColumnName(keyName)).append("=?");
        return execute(sql.toString(), new Object[] { key });
    }

    public int removeByKeys(String table, String keyName, Collection<?> keys) {
        StringBuilder sql = new StringBuilder("delete from ").append(table).append(" where ")
                .append(convertColumnName(keyName)).append(" in(");
        for (int i = 0; i < keys.size(); i++) {
            if (i > 0) {
                sql.append(",?");
            } else {
                sql.append('?');
            }
        }
        sql.append(')');
        return execute(sql.toString(), keys.toArray(new Object[keys.size()]));
    }
}
