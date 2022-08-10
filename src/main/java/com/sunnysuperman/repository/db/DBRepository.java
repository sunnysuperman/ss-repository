package com.sunnysuperman.repository.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;

import com.sunnysuperman.commons.page.Page;
import com.sunnysuperman.commons.page.PullPage;
import com.sunnysuperman.commons.util.FormatUtil;
import com.sunnysuperman.repository.InsertUpdate;
import com.sunnysuperman.repository.RepositoryException;
import com.sunnysuperman.repository.db.mapper.DBMapper;
import com.sunnysuperman.repository.db.mapper.ObjectDBMapper;

public abstract class DBRepository {

	protected abstract JdbcTemplate getJdbcTemplate();

	protected String convertColumnName(String columnName) {
		return columnName;
	}

	protected String getInsertDialect(Map<String, ?> kv, String tableName, List<Object> params) {
		StringBuilder buf = new StringBuilder("insert into ").append(tableName).append('(');

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

	protected String getPagingDialect(String sql, int offset, int limit) {
		if (limit <= 0) {
			return sql;
		}
		return new StringBuilder(sql).append(" limit ").append(offset).append(",").append(limit).toString();
	}

	protected String getCountDialect(String sql) {
		int index1 = sql.indexOf(" from ");
		if (index1 <= 0) {
			throw new RuntimeException("Bad sql: " + sql);
		}
		int index2 = sql.indexOf(" order by", index1);
		if (index2 < 0) {
			index2 = sql.length();
		}
		StringBuilder buf = new StringBuilder("select count(*)").append(sql.substring(index1, index2));
		return buf.toString();
	}

	protected String getExecuteLimitDialect(String sql, int limit) {
		return sql + " limit " + limit;
	}

	public int execute(String sql, Object[] params) {
		return getJdbcTemplate().update(sql, params);
	}

	public int execute(String sql, Object[] params, int limit) {
		return execute(getExecuteLimitDialect(sql, limit), params);
	}

	public int[] executeBatch(String sql, List<Object[]> batchParams) {
		return getJdbcTemplate().batchUpdate(sql, batchParams);
	}

	public boolean insertDoc(String tableName, Map<String, ?> doc) {
		List<Object> params = new ArrayList<Object>(doc.size());
		String sql = getInsertDialect(doc, tableName, params);
		return execute(sql, params.toArray(new Object[params.size()])) > 0;
	}

	@SuppressWarnings("unchecked")
	public <T extends Number> T insertDoc(String tableName, Map<String, ?> doc, Class<T> generatedKeyClass) {
		List<Object> params = new ArrayList<Object>(doc.size());
		String sql = getInsertDialect(doc, tableName, params);
		Object[] paramsArray = params.toArray(new Object[params.size()]);
		GeneratedKeyHolder holder = new GeneratedKeyHolder();
		boolean inserted = getJdbcTemplate().update(new CreateWithGeneratedKeyStatement(sql, paramsArray), holder) > 0;
		if (!inserted) {
			// should not happened
			throw new RepositoryException("Insert error");
		}
		if (generatedKeyClass == null || generatedKeyClass == Number.class) {
			return (T) holder.getKey();
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

	public void insertDocs(String tableName, List<Map<String, Object>> docs) {
		if (docs.size() == 1) {
			insertDoc(tableName, docs.get(0));
			return;
		}
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
				params[i] = doc.get(keys[i]);
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

	public InsertUpdate saveDoc(String tableName, Map<String, Object> doc, String primaryKey) {
		Object primaryValue = doc.remove(primaryKey);
		if (primaryValue == null) {
			boolean inserted = insertDoc(tableName, doc);
			return inserted ? InsertUpdate.INSERT : null;
		}
		boolean updated = updateDoc(tableName, doc, primaryKey, primaryValue) > 0;
		if (updated) {
			return InsertUpdate.UPDATE;
		}
		doc.put(primaryKey, primaryValue);
		boolean inserted = insertDoc(tableName, doc);
		return inserted ? InsertUpdate.INSERT : null;
	}

	public InsertUpdate saveDoc(String tableName, Map<String, Object> doc) {
		return saveDoc(tableName, doc, "id");
	}

	public <T> T find(String sql, Object[] params, DBMapper<T> mapper) {
		List<Map<String, Object>> rawItems = getJdbcTemplate().queryForList(getPagingDialect(sql, 0, 1), params);
		if (rawItems.isEmpty()) {
			return null;
		}
		Map<String, Object> rawItem = rawItems.get(0);
		return mapper.map(rawItem);
	}

	public <T> List<T> findForList(String sql, Object[] params, int offset, int limit, DBMapper<T> mapper) {
		List<Map<String, Object>> rawItems = getJdbcTemplate().queryForList(getPagingDialect(sql, offset, limit),
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

	public <T> Set<T> findForSet(String sql, Object[] params, int offset, int limit, DBMapper<T> mapper) {
		List<Map<String, Object>> rawItems = getJdbcTemplate().queryForList(getPagingDialect(sql, offset, limit),
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

	public <T> Page<T> findForPage(String sql, Object[] params, int offset, int limit, DBMapper<T> mapper) {
		List<T> items = findForList(sql, params, offset, limit, mapper);
		int size = items.size();
		if (size == 0) {
			return Page.empty(limit);
		}
		if (offset != 0 || size == limit) {
			size = count(getCountDialect(sql), params);
		}
		return new Page<T>(items, size, offset, limit);
	}

	public <T> Page<T> findForPage(String sql, String countSql, Object[] params, int offset, int limit,
			DBMapper<T> mapper) {
		List<T> items = findForList(sql, params, offset, limit, mapper);
		int size = items.size();
		if (size == 0) {
			return Page.empty(limit);
		}
		if (offset != 0 || size == limit) {
			size = count(countSql != null ? countSql : getCountDialect(sql), params);
		}
		return new Page<T>(items, size, offset, limit);
	}

	public <T> PullPage<T> findForPullPage(String sql, Object[] params, String marker, int limit, DBMapper<T> mapper) {
		int offset = marker == null ? 0 : Integer.parseInt(marker);
		List<Map<String, Object>> rawItems = getJdbcTemplate().queryForList(getPagingDialect(sql, offset, limit + 1),
				params);
		if (rawItems.isEmpty()) {
			return PullPage.empty();
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
		return PullPage.of(items, String.valueOf(newOffset), hasMore);
	}

	public <T> PullPage<T> findForPullPageByColumn(String sql, Object[] params, String column, int limit,
			DBMapper<T> mapper) {
		List<Map<String, Object>> rawItems = getJdbcTemplate().queryForList(getPagingDialect(sql, 0, limit + 1),
				params);
		if (rawItems.isEmpty()) {
			return PullPage.empty();
		}
		List<T> items = new ArrayList<>(Math.min(limit, rawItems.size()));
		boolean hasMore = rawItems.size() > limit;
		String newMarker = null;
		if (hasMore) {
			newMarker = rawItems.get(limit - 1).get(column).toString();
		}
		for (Map<String, Object> rawItem : rawItems) {
			T item = mapper.map(rawItem);
			items.add(item);
			if (items.size() >= limit) {
				break;
			}
		}
		return PullPage.of(items, newMarker, hasMore);
	}

	public int count(String sql, Object[] params) {
		Integer val = getJdbcTemplate().queryForObject(sql, params, Integer.class);
		return FormatUtil.parseIntValue(val, 0);
	}

	public boolean exists(String sql, Object[] params) {
		return find(sql, params, ObjectDBMapper.getInstance()) != null;
	}

}
