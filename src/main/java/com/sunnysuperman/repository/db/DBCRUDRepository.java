package com.sunnysuperman.repository.db;

import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.sunnysuperman.commons.page.Page;
import com.sunnysuperman.commons.page.PageRequest;
import com.sunnysuperman.commons.page.PullPage;
import com.sunnysuperman.commons.page.PullPageRequest;
import com.sunnysuperman.commons.util.StringUtil;
import com.sunnysuperman.repository.CRUDRepository;
import com.sunnysuperman.repository.FieldValue;
import com.sunnysuperman.repository.InsertUpdate;
import com.sunnysuperman.repository.RepositoryException;
import com.sunnysuperman.repository.SaveResult;
import com.sunnysuperman.repository.db.mapper.DBMapper;
import com.sunnysuperman.repository.db.mapper.ObjectDBMapper;
import com.sunnysuperman.repository.exception.StaleEntityRepositoryException;

public abstract class DBCRUDRepository<T, ID> extends DBRepository implements CRUDRepository<T, ID> {
	private Class<T> entityClass;
	private DBMapper<T> entityMapper;
	private String table;
	private String findByIdSql;
	private String findAllSql;
	private String deleteByIdSql;
	private VersionAwareSql deleteByIdAndVersionSql;
	private String existsByIdSql;
	private Map<String, String> fieldColumnMapping = new ConcurrentHashMap<>();

	private final class TheEntityMapper implements DBMapper<T> {

		@Override
		public T map(Map<String, Object> row) {
			return EntityManager.deserialize(row, getEntityClass(), getDefaultFieldConverter());
		}

	}

	@SuppressWarnings("unchecked")
	protected Class<T> getEntityClass() {
		if (entityClass == null) {
			entityClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass())
					.getActualTypeArguments()[0];
		}
		return entityClass;
	}

	protected DBMapper<T> getEntityMapper() {
		if (entityMapper == null) {
			entityMapper = new TheEntityMapper();
		}
		return entityMapper;
	}

	protected final String getTable() {
		if (table == null) {
			table = EntityManager.getTable(getEntityClass());
		}
		return table;
	}

	protected final String getColumnsByFields(String fields) {
		if (fields == null) {
			throw new IllegalArgumentException("fields");
		}
		String columns = fieldColumnMapping.get(fields);
		if (columns == null) {
			Set<String> fieldSet = new HashSet<>(StringUtil.split(fields, ","));
			Set<String> columnSet = EntityManager.findColumnNames(getEntityClass(), fieldSet);
			if (columnSet.size() < fieldSet.size()) {
				throw new RepositoryException("Some columns not found for " + StringUtil.join(fields));
			}
			columns = StringUtil.join(columnSet);
			fieldColumnMapping.put(fields, columns);
		}
		return columns;
	}

	@SuppressWarnings("unchecked")
	protected final Map<ID, T> list2map(List<T> list) {
		if (list.isEmpty()) {
			return Collections.emptyMap();
		}
		Map<ID, T> map = new HashMap<>();
		for (T item : list) {
			map.put((ID) EntityManager.getEntityId(item), item);
		}
		return map;
	}

	protected DefaultFieldConverter getDefaultFieldConverter() {
		return BuildInDefautFieldConverter.getInstance();
	}

	private static StringBuilder appendInClause(StringBuilder sql, Collection<?> items) {
		sql.append(" in(");
		for (int i = 0; i < items.size(); i++) {
			if (i > 0) {
				sql.append(",?");
			} else {
				sql.append('?');
			}
		}
		sql.append(')');
		return sql;
	}

	private SaveResult save(T entity, Set<String> fields, InsertUpdate insertUpdate) {
		SerializedRow row;
		try {
			row = EntityManager.serialize(entity, fields, insertUpdate, getDefaultFieldConverter());
		} catch (Exception ex) {
			throw new RepositoryException(ex);
		}
		String tableName = row.getTableName();
		SaveResult result = new SaveResult();
		Map<String, Object> data = row.getData();
		try {
			// insert
			if (insertUpdate == InsertUpdate.INSERT || row.getIdValues() == null) {
				if (row.isIdGeneration()) {
					Number id = insertDoc(tableName, data, Number.class);
					if (id == null) {
						throw new RepositoryException("Failed to insert and generate id");
					}
					// 设置ID到实体，同时回显到结果里
					result.setGeneratedId(EntityManager.setEntityId(entity, id, getDefaultFieldConverter()));
					result.setInserted(true);
				} else {
					boolean inserted = insertDoc(tableName, data);
					result.setInserted(inserted);
				}
				return result;
			}
			// update
			if (row.getIdValues() == null) {
				throw new RepositoryException("Require id to update");
			}
			boolean updated = updateDoc(tableName, data, row.getIdColumns(), row.getIdValues()) > 0;
			if (updated || insertUpdate == InsertUpdate.UPDATE) {
				if (!updated && row.isVersioning()) {
					throw new StaleEntityRepositoryException("Failed to update entity for "
							+ StringUtil.join(row.getIdColumns()) + "/" + StringUtil.join(row.getIdValues()));
				}
				result.setUpdated(updated);
				return result;
			}
			// upsert
			Map<String, Object> upsertData = row.getUpsertData();
			if (upsertData != null) {
				boolean inserted = insertDoc(tableName, upsertData);
				result.setInserted(inserted);
			}
			return result;
		} finally {
			// 保存成功，版本号需要更新到实体
			if (row.isVersioning() && result.success()) {
				if (result.isUpdated()) {
					EntityManager.setVersionValue(entity, row.getUpdatedVersion());
				} else if (row.getInsertedVersion() != null) {
					EntityManager.setVersionValue(entity, row.getInsertedVersion());
				}
			}
		}
	}

	@Override
	public SaveResult save(T entity) throws RepositoryException {
		return save(entity, null, InsertUpdate.UPSERT);
	}

	@Override
	public void insert(T entity) throws RepositoryException {
		save(entity, null, InsertUpdate.INSERT);
	}

	@Override
	public void insertBatch(List<T> entityList) throws RepositoryException {
		List<Map<String, Object>> docs = new ArrayList<>(entityList.size());
		boolean idGeneration = false;
		for (T item : entityList) {
			SerializedRow row = EntityManager.serialize(item, null, InsertUpdate.INSERT, getDefaultFieldConverter());
			idGeneration = row.isIdGeneration();
			Map<String, Object> doc = row.getData();
			docs.add(doc);
		}
		if (!idGeneration) {
			insertDocs(getTable(), docs);
			return;
		}
		List<Number> ids = insertDocs(getTable(), docs, Number.class);
		if (ids == null) {
			return;
		}
		for (int i = 0; i < entityList.size(); i++) {
			EntityManager.setEntityId(entityList.get(i), ids.get(i), getDefaultFieldConverter());
		}
	}

	@Override
	public boolean update(T entity) throws RepositoryException {
		return save(entity, null, InsertUpdate.UPDATE).isUpdated();
	}

	@Override
	public boolean update(T entity, Set<String> fields) throws RepositoryException {
		return save(entity, fields, InsertUpdate.UPDATE).isUpdated();
	}

	@Override
	public boolean deleteById(ID id) throws RepositoryException {
		if (deleteByIdSql == null) {
			deleteByIdSql = new StringBuilder("delete from ").append(getTable()).append(" where ")
					.append(EntityManager.getIdColumnName(getEntityClass())).append("=?").toString();
		}
		return execute(deleteByIdSql, new Object[] { id }) > 0;
	}

	@Override
	public int deleteByIds(Collection<ID> ids) throws RepositoryException {
		if (ids.size() == 1) {
			return deleteById(ids.iterator().next()) ? 1 : 0;
		}
		StringBuilder sql = new StringBuilder("delete from ").append(getTable()).append(" where ")
				.append(EntityManager.getIdColumnName(getEntityClass()));
		appendInClause(sql, ids);
		return execute(sql.toString(), ids.toArray());
	}

	@Override
	public boolean delete(T entity) throws RepositoryException {
		FieldValue idField = EntityManager.findIdFieldAndValue(entity, getDefaultFieldConverter());
		FieldValue versionField = EntityManager.findVersionFieldAndValue(entity, getDefaultFieldConverter());
		// SQL缓存
		if (deleteByIdAndVersionSql == null) {
			StringBuilder sql = new StringBuilder("delete from ").append(getTable()).append(" where ")
					.append(idField.getColumnName()).append("=?");
			if (versionField != null) {
				sql.append(" and ").append(versionField.getColumnName()).append("=?");
			}
			deleteByIdAndVersionSql = new VersionAwareSql(sql.toString(), versionField != null);
		}
		Object[] params;
		if (deleteByIdAndVersionSql.isHasVesion()) {
			params = new Object[] { idField.getColumnValue(), versionField.getColumnValue() };
		} else {
			params = new Object[] { idField.getColumnValue() };
		}
		return execute(deleteByIdAndVersionSql.getSql(), params) > 0;
	}

	@Override
	public boolean existsById(ID id) throws RepositoryException {
		if (existsByIdSql == null) {
			existsByIdSql = new StringBuilder("select 1 from ").append(getTable()).append(" where ")
					.append(EntityManager.getIdColumnName(getEntityClass())).append("=?").toString();
		}
		return find(existsByIdSql, new Object[] { id }, ObjectDBMapper.getInstance()) != null;
	}

	@Override
	public T findById(ID id) throws RepositoryException {
		if (findByIdSql == null) {
			findByIdSql = new StringBuilder("select * from ").append(getTable()).append(" where ")
					.append(EntityManager.getIdColumnName(getEntityClass())).append("=?").toString();
		}
		return find(findByIdSql, new Object[] { id }, getEntityMapper());
	}

	@Override
	public List<T> findByIds(Collection<ID> ids) throws RepositoryException {
		StringBuilder sql = new StringBuilder("select * from ").append(getTable()).append(" where ")
				.append(EntityManager.getIdColumnName(getEntityClass()));
		appendInClause(sql, ids);
		return findForList(sql.toString(), ids.toArray(), 0, 0, getEntityMapper());
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<T> findByIdsInOrder(Collection<ID> ids) throws RepositoryException {
		List<T> list = findByIds(ids);
		if (list.isEmpty()) {
			return Collections.emptyList();
		}
		Map<ID, T> map = new HashMap<>();
		for (T item : list) {
			map.put((ID) EntityManager.getEntityId(item), item);
		}
		List<T> listInOrder = new ArrayList<>(list.size());
		for (ID id : ids) {
			T item = map.get(id);
			if (item != null) {
				listInOrder.add(item);
			}
		}
		return listInOrder;
	}

	@Override
	public Map<ID, T> findByIdsAsMap(Collection<ID> ids) throws RepositoryException {
		List<T> list = findByIds(ids);
		return list2map(list);
	}

	@Override
	public List<T> findAll() {
		if (findAllSql == null) {
			findAllSql = new StringBuilder("select * from ").append(getTable()).toString();
		}
		int max = maxItemsForFindAll();
		List<T> items = findForList(findAllSql, null, 0, max + 1, getEntityMapper());
		if (max > 0 && items.size() > max) {
			throw new RepositoryException("To prevent from OOM, we could not load more than " + max + " items");
		}
		return items;
	}

	protected int maxItemsForFindAll() {
		return 10000;
	}

	protected final int updateDoc(String tableName, Map<String, Object> doc, String key, Object value,
			DefaultFieldConverter converter) {
		for (Entry<String, Object> entry : doc.entrySet()) {
			Object val = entry.getValue();
			if (val != null) {
				entry.setValue(converter.convertToColumn(val));
			}
		}
		return super.updateDoc(tableName, doc, key, value);
	}

	protected final T find(String sql, Object[] params) {
		return find(sql, params, getEntityMapper());
	}

	protected final Page<T> findForPage(String sql, Object[] params, PageRequest page) {
		return findForPage(sql, params, page.getOffset(), page.getLimit(), getEntityMapper());
	}

	protected final Page<T> findForPage(String sql, String countSql, Object[] params, PageRequest page) {
		return findForPage(sql, countSql, params, page.getOffset(), page.getLimit(), getEntityMapper());
	}

	protected final PullPage<T> findForPullPage(String sql, Object[] params, PullPageRequest page) {
		return findForPullPage(sql, params, page.getMarker(), page.getLimit(), getEntityMapper());
	}

	protected final PullPage<T> findForPullPageByColumn(String sql, Object[] params, String column,
			PullPageRequest page) {
		return findForPullPageByColumn(sql, params, column, page.getLimit(), getEntityMapper());
	}

	protected final List<T> findForList(String sql, Object[] params, int offset, int limit) {
		return findForList(sql, params, offset, limit, getEntityMapper());
	}

	protected final List<T> findForList(String sql, Object[] params) {
		return findForList(sql, params, 0, 0, getEntityMapper());
	}

	protected final Set<T> findForSet(String sql, Object[] params, int offset, int limit) {
		return findForSet(sql, params, offset, limit, getEntityMapper());
	}

}
