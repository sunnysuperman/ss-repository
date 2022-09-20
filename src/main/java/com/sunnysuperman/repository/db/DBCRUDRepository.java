package com.sunnysuperman.repository.db;

import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.sunnysuperman.commons.page.Page;
import com.sunnysuperman.commons.page.PageRequest;
import com.sunnysuperman.commons.page.PullPage;
import com.sunnysuperman.commons.page.PullPageRequest;
import com.sunnysuperman.repository.CRUDRepository;
import com.sunnysuperman.repository.InsertUpdate;
import com.sunnysuperman.repository.RepositoryException;
import com.sunnysuperman.repository.SaveResult;
import com.sunnysuperman.repository.db.mapper.DBMapper;
import com.sunnysuperman.repository.db.mapper.ObjectDBMapper;

public abstract class DBCRUDRepository<T, ID> extends DBRepository implements CRUDRepository<T, ID> {
	private Class<T> entityClass;
	private DBMapper<T> entityMapper;
	private String table;
	private String findByIdSql;
	private String deleteByIdSql;
	private String existsByIdSql;

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

	protected DefautFieldConverter getDefaultFieldConverter() {
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
		// insert
		if (insertUpdate == InsertUpdate.INSERT || row.getIdValues() == null) {
			if (row.isIdGeneration()) {
				Number id = insertDoc(tableName, data, Number.class);
				if (id == null) {
					throw new RepositoryException("Failed to insert and generate id");
				}
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
			result.setUpdated(updated);
			return result;
		}
		Map<String, Object> upsertData = row.getUpsertData();
		if (upsertData == null) {
			return result;
		}
		// upsert
		boolean inserted = insertDoc(tableName, upsertData);
		result.setInserted(inserted);
		return result;
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
		if (entityList.size() == 1) {
			insert(entityList.get(0));
			return;
		}
		List<Map<String, Object>> docs = new ArrayList<>(entityList.size());
		for (T item : entityList) {
			Map<String, Object> doc = EntityManager
					.serialize(item, null, InsertUpdate.INSERT, getDefaultFieldConverter()).getData();
			docs.add(doc);
		}
		insertDocs(getTable(), docs);
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

	protected final T find(String sql, Object[] params) {
		return find(sql, params, getEntityMapper());
	}

	protected final Page<T> findForPage(String sql, Object[] params, PageRequest page) {
		return findForPage(sql, params, page.getOffset(), page.getLimit(), getEntityMapper());
	}

	protected final PullPage<T> findForPullPage(String sql, Object[] params, PullPageRequest page) {
		return findForPullPage(sql, params, page.getMarker(), page.getLimit(), getEntityMapper());
	}

	protected final PullPage<T> findForPullPageByColumn(String sql, Object[] params, String column,
			PullPageRequest page) {
		return findForPullPageByColumn(sql, params, column, page.getLimit(), getEntityMapper());
	}

}
