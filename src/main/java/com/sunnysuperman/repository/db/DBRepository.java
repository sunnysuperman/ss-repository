package com.sunnysuperman.repository.db;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
import com.sunnysuperman.repository.serialize.SerializeBeanUtils;
import com.sunnysuperman.repository.serialize.SerializeDoc;

public abstract class DBRepository {

    protected abstract JdbcTemplate getJdbcTemplate();

    private static boolean isSimpleType(Class<?> type) {
        return (type.isPrimitive() && type != void.class) || type == Double.class || type == Float.class
                || type == Long.class || type == Integer.class || type == Short.class || type == Character.class
                || type == Byte.class || type == Boolean.class || type == String.class;
    }

    protected static Object serializeObject(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Date) {
            return ((Date) value).getTime();
        }
        if (isSimpleType(value.getClass())) {
            return value;
        }
        return JSONUtil.toJSONString(value);
    }

    protected static String getInsertDialect(Map<String, ?> kv, String tableName, List<Object> params, boolean ignore) {
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
            buf.append(entry.getKey());
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

    protected static String getPaginationDialect(String sql, int offset, int limit) {
        if (limit <= 0) {
            return sql;
        }
        return new StringBuilder(sql).append(" limit ").append(offset).append(",").append(limit).toString();
    }

    protected static String getCountDialect(String sql) {
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

    protected static Object[] serializeParams(Object[] params) {
        if (params == null || params.length == 0) {
            return null;
        }
        for (int i = 0; i < params.length; i++) {
            params[i] = serializeObject(params[i]);
        }
        return params;
    }

    public int execute(String sql, Object[] params) {
        return getJdbcTemplate().update(sql, serializeParams(params));
    }

    public boolean insert(String tableName, Map<String, ?> doc, boolean ignore) {
        List<Object> params = new ArrayList<Object>(doc.size());
        String sql = getInsertDialect(doc, tableName, params, ignore);
        return execute(sql, params.toArray(new Object[params.size()])) > 0;
    }

    public boolean insert(String tableName, Map<String, ?> doc) {
        return insert(tableName, doc, false);
    }

    @SuppressWarnings("unchecked")
    public <T extends Number> T insert(String tableName, Map<String, ?> doc, boolean ignore,
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

    public int update(String tableName, Map<String, ?> doc, Object primaryKey, Object primaryValue) {
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
                        String incKey = incEntry.getKey().toString();
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
            if (offset > 0) {
                buf.append(",");
            }
            offset++;
            buf.append(fieldKey);
            if (fieldValue != null && fieldValue instanceof DBFunction) {
                // column=ST_GeometryFromText(?,4326), column=point(?,?), etc.
                DBFunction cf = (DBFunction) fieldValue;
                buf.append("=").append(cf.getFunction());
                if (cf.getParams() != null) {
                    for (Object param : cf.getParams()) {
                        params.add(param);
                    }
                }
            } else {
                buf.append("=?");
                params.add(fieldValue);
            }
        }
        buf.append(" where ");
        if (primaryKey.getClass().isArray()) {
            // where pk1=? and pk2=?
            int length = Array.getLength(primaryKey);
            for (int i = 0; i < length; i++) {
                Object pk = Array.get(primaryKey, i);
                if (i > 0) {
                    buf.append(" and ");
                }
                buf.append(pk);
                buf.append("=?");
                params.add(Array.get(primaryValue, i));
            }
        } else {
            buf.append(primaryKey);
            buf.append("=?");
            params.add(primaryValue);
        }
        Object[] paramsAsArray = params.toArray(new Object[params.size()]);
        return execute(buf.toString(), paramsAsArray);
    }

    public SaveResult save(Object bean) {
        return save(bean, InsertUpdate.RUNTIME);
    }

    public SaveResult save(Object bean, InsertUpdate insertUpdate) {
        String tableName = bean.getClass().getAnnotation(SerializeBean.class).value();
        SerializeDoc sdoc;
        try {
            sdoc = SerializeBeanUtils.serialize(bean, insertUpdate);
        } catch (Exception ex) {
            throw new RepositoryException(ex);
        }
        SaveResult result = new SaveResult();
        if (sdoc.getIdValues() != null) {
            boolean updated = update(tableName, sdoc.getDoc(), sdoc.getIdColumns(), sdoc.getIdValues()) > 0;
            if (!updated && sdoc.isUpsert()) {
                for (int i = 0; i < sdoc.getIdColumns().length; i++) {
                    sdoc.getDoc().put(sdoc.getIdColumns()[i], sdoc.getIdValues()[i]);
                }
                boolean inserted = insert(tableName, sdoc.getDoc());
                result.setInserted(inserted);
            } else {
                result.setUpdated(updated);
            }
        } else {
            Class<? extends Number> idIncrementClass = sdoc.getIdIncrementClass();
            Number id;
            if (idIncrementClass != null) {
                id = insert(tableName, sdoc.getDoc(), false, idIncrementClass);
                if (id == null) {
                    throw new RepositoryException("Failed to insert");
                }
                result.setInserted(true);
                result.setGeneratedId(id);
                SerializeBeanUtils.setIncrementId(bean, id);
            } else {
                boolean inserted = insert(tableName, sdoc.getDoc());
                result.setInserted(inserted);
            }
        }
        return result;
    }

    public InsertUpdate save(String tableName, Map<String, Object> doc, String primaryKey, boolean ignore) {
        Object primaryValue = doc.remove(primaryKey);
        boolean updated = update(tableName, doc, primaryKey, primaryValue) > 0;
        if (updated) {
            return InsertUpdate.UPDATE;
        }
        doc.put(primaryKey, primaryValue);
        boolean inserted = insert(tableName, doc, ignore);
        return inserted ? InsertUpdate.INSERT : null;
    }

    public <T> T findOne(String sql, Object[] params, DBRowMapper<T> mapper) {
        List<Map<String, Object>> rawItems = getJdbcTemplate().queryForList(getPaginationDialect(sql, 0, 1), params);
        if (rawItems.isEmpty()) {
            return null;
        }
        Map<String, Object> rawItem = rawItems.get(0);
        return mapper.map(rawItem);
    }

    public <T> List<T> findList(String sql, Object[] params, int offset, int limit, DBRowMapper<T> mapper) {
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

    public int count(String sql, Object[] params) {
        Integer val = getJdbcTemplate().queryForObject(sql, params, Integer.class);
        return FormatUtil.parseIntValue(val, 0);
    }

    public <T> PullPagination<T> findPullPagination(String sql, Object[] params, String marker, int limit,
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

    public <T> Pagination<T> findPagination(String sql, Object[] params, int offset, int limit, DBRowMapper<T> mapper) {
        List<T> items = findList(sql, params, offset, limit, mapper);
        int size = items.size();
        if (size == 0) {
            return Pagination.emptyInstance(limit);
        }
        if (offset != 0 || size == limit) {
            size = count(getCountDialect(sql), params);
        }
        return new Pagination<T>(items, size, offset, limit);
    }

    public int removeByKey(String table, String keyName, Object key) {
        StringBuilder sql = new StringBuilder("delete from ").append(table).append(" where ").append(keyName)
                .append("=?");
        return execute(sql.toString(), new Object[] { key });
    }

    public int removeByKeys(String table, String keyName, Collection<?> keys) {
        StringBuilder sql = new StringBuilder("delete from ").append(table).append(" where ").append(keyName)
                .append(" in(");
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

    public <T> T findByKey(String table, String selectKeys, String keyName, Object key, DBRowMapper<T> mapper) {
        StringBuilder sql = new StringBuilder("select ").append(selectKeys != null ? selectKeys : "*").append(" from ")
                .append(table).append(" where ").append(keyName).append("=?");
        return findOne(sql.toString(), new Object[] { key }, mapper);
    }

    public <T> List<T> findByKeys(String table, String selectKeys, String keyName, Collection<?> keys,
            DBRowMapper<T> mapper) {
        StringBuilder sql = new StringBuilder("select ").append(selectKeys != null ? selectKeys : "*").append(" from ")
                .append(table).append(" where ").append(keyName).append(" in(");
        for (int i = 0; i < keys.size(); i++) {
            if (i > 0) {
                sql.append(",?");
            } else {
                sql.append('?');
            }
        }
        sql.append(')');
        return findList(sql.toString(), keys.toArray(new Object[keys.size()]), 0, 0, mapper);
    }
}
