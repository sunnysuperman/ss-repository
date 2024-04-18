package com.sunnysuperman.repository.db;

import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.sunnysuperman.commons.page.Page;
import com.sunnysuperman.commons.page.PageRequest;
import com.sunnysuperman.commons.page.PullPage;
import com.sunnysuperman.commons.page.PullPageRequest;
import com.sunnysuperman.commons.util.StringUtil;
import com.sunnysuperman.repository.CRUDRepository;
import com.sunnysuperman.repository.FieldValue;
import com.sunnysuperman.repository.RepositoryException;
import com.sunnysuperman.repository.SaveResult;
import com.sunnysuperman.repository.annotation.IdStrategy;
import com.sunnysuperman.repository.db.mapper.DBMapper;
import com.sunnysuperman.repository.db.mapper.EntityMapper;
import com.sunnysuperman.repository.db.mapper.ObjectDBMapper;
import com.sunnysuperman.repository.exception.StaleEntityRepositoryException;

public abstract class DBCRUDRepository<T, I> extends DBRepository implements CRUDRepository<T, I> {
	private Class<T> entityClass;
	private DBMapper<T> entityMapper;
	private EntityMeta entityMeta;
	private String findByIdSql;
	private String findAllSql;
	private String deleteByIdSql;
	private String deleteByIdAndVersionSql;
	private String existsByIdSql;
	private Map<String, String> fieldColumnMapping = new ConcurrentHashMap<>();

	@SuppressWarnings("unchecked")
	protected Class<T> getEntityClass() {
		if (entityClass == null) {
			entityClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass())
					.getActualTypeArguments()[0];
		}
		return entityClass;
	}

	protected final DBMapper<T> getEntityMapper() {
		if (entityMapper == null) {
			entityMapper = new EntityMapper<>(getEntityClass(), getDefaultFieldConverter());
		}
		return entityMapper;
	}

	protected final <M> DBMapper<M> getEntityMapper(Class<M> clazz) {
		return new EntityMapper<>(clazz, getDefaultFieldConverter());
	}

	protected final String getTable() {
		return getEntityMeta().getTableName();
	}

	protected final String getColumnsByFields(String fields) {
		if (fields == null) {
			throw new IllegalArgumentException("fields");
		}
		String columns = fieldColumnMapping.get(fields);
		if (columns == null) {
			Set<String> fieldSet = new HashSet<>(StringUtil.split(fields, ","));
			Set<String> columnSet = getEntityMeta().getColumnNames(fieldSet);
			if (columnSet.size() < fieldSet.size()) {
				throw new RepositoryException("Some columns not found for " + StringUtil.join(fields));
			}
			columns = StringUtil.join(columnSet);
			fieldColumnMapping.put(fields, columns);
		}
		return columns;
	}

	@SuppressWarnings("unchecked")
	protected final Map<I, T> list2map(List<T> list) {
		if (list.isEmpty()) {
			return Collections.emptyMap();
		}
		Map<I, T> map = new HashMap<>();
		for (T item : list) {
			map.put((I) getEntityMeta().getEntityId(item), item);
		}
		return map;
	}

	protected DefaultFieldConverter getDefaultFieldConverter() {
		return BuildInDefautFieldConverter.getInstance();
	}

	@Override
	public SaveResult save(T entity) throws RepositoryException {
		EntityMeta meta = getEntityMeta();
		Object entityId = meta.getEntityId(entity);
		if (entityId == null) {
			insert(entity);
			return SaveResult.RES_INSERTED;
		}
		boolean upsert = meta.getIdInfo().strategy() == IdStrategy.PROVIDED;
		if (upsert) {
			if (doUpdate(Collections.singletonList(entity), null, true)) {
				return SaveResult.RES_UPDATED;
			}
			insert(entity);
			return SaveResult.RES_INSERTED;
		}
		if (doUpdate(Collections.singletonList(entity), null, false)) {
			return SaveResult.RES_UPDATED;
		}
		return SaveResult.RES_NONE;
	}

	@Override
	public void insert(T entity) throws RepositoryException {
		doInsert(entity);
	}

	@Override
	public void insertBatch(List<T> entityList) throws RepositoryException {
		if (entityList.size() <= 1) {
			doInsert(entityList.get(0));
			return;
		}
		EntityMeta meta = getEntityMeta();
		DefaultFieldConverter converter = getDefaultFieldConverter();
		String sql = meta.getInsertSql(entityList.get(0), converter);
		SaveParams insertParams = meta.getInsertParams(entityList, converter);
		Class<?> generatedIdClass = meta.findGeneratedIdFieldType();
		if (generatedIdClass == null) {
			executeBatch(sql, insertParams.getParams());
		} else {
			List<?> generatedIds = getJdbcTemplate().execute(new GeneratKeysPreparedStatementCreator(sql),
					new InsertBatchPreparedStatementCallback<>(insertParams.getParams(), generatedIdClass));
			if (generatedIds == null || generatedIds.isEmpty()) {
				throw new RepositoryException("No id generated");
			}
			// 生成ID回传
			for (int i = 0; i < entityList.size(); i++) {
				meta.setEntityId(entityList.get(i), generatedIds.get(i), converter);
			}
		}
		// 版本号回传
		if (insertParams.versioning()) {
			for (int i = 0; i < entityList.size(); i++) {
				meta.setVersionValue(entityList.get(i), insertParams.getNewVersions().get(i));
			}
		}
	}

	@Override
	public boolean update(T entity) throws RepositoryException {
		return doUpdate(Collections.singletonList(entity), null, false);
	}

	@Override
	public boolean update(T entity, Set<String> fields) throws RepositoryException {
		return doUpdate(Collections.singletonList(entity), fields, false);
	}

	@Override
	public boolean updateBatch(List<T> entityList) throws RepositoryException {
		return doUpdate(entityList, null, false);
	}

	@Override
	public boolean updateBatch(List<T> entityList, Set<String> fields) throws RepositoryException {
		return doUpdate(entityList, fields, false);
	}

	@Override
	public void compareAndUpdateVersion(T entity) throws RepositoryException {
		if (!doUpdate(Collections.singletonList(entity), Collections.emptySet(), false)) {
			throw new RepositoryException("compareAndUpdateVersion failed");
		}
	}

	@Override
	public boolean deleteById(I id) throws RepositoryException {
		if (deleteByIdSql == null) {
			deleteByIdSql = makeDeleteByIdSql().append("=?").toString();
		}
		return execute(deleteByIdSql, new Object[] { id }) > 0;
	}

	@Override
	public int deleteByIds(Collection<I> ids) throws RepositoryException {
		if (ids.size() == 1) {
			return deleteById(ids.iterator().next()) ? 1 : 0;
		}
		StringBuilder sql = makeDeleteByIdSql();
		appendInClause(sql, ids);
		return execute(sql.toString(), ids.toArray());
	}

	@Override
	public boolean delete(T entity) throws RepositoryException {
		EntityMeta meta = getEntityMeta();
		FieldValue idField = meta.findIdFieldAndValue(entity, getDefaultFieldConverter());
		FieldValue versionField = meta.findVersionFieldAndValue(entity, getDefaultFieldConverter());
		// SQL缓存
		if (deleteByIdAndVersionSql == null) {
			StringBuilder sql = makeDeleteByIdSql().append("=?");
			if (versionField != null) {
				sql.append(" and ").append(versionField.getColumnName()).append("=?");
			}
			deleteByIdAndVersionSql = sql.toString();
		}
		Object[] params;
		if (versionField != null) {
			params = new Object[] { idField.getColumnValue(), versionField.getColumnValue() };
		} else {
			params = new Object[] { idField.getColumnValue() };
		}
		boolean updated = execute(deleteByIdAndVersionSql, params) > 0;
		if (!updated && versionField != null) {
			throw new StaleEntityRepositoryException("Failed to delete entity for " + entity.getClass() + "/"
					+ idField.getColumnValue() + ", maybe entity is stale");
		}
		return updated;
	}

	@Override
	public boolean existsById(I id) throws RepositoryException {
		if (existsByIdSql == null) {
			existsByIdSql = new StringBuilder("select 1 from ").append(getTable()).append(" where ")
					.append(getEntityMeta().getIdColumnName()).append("=?").toString();
		}
		return find(existsByIdSql, new Object[] { id }, ObjectDBMapper.getInstance()) != null;
	}

	@Override
	public T findById(I id) throws RepositoryException {
		Objects.requireNonNull(id);
		if (findByIdSql == null) {
			findByIdSql = makeFindByIdSql().append("=?").toString();
		}
		return find(findByIdSql, new Object[] { id }, getEntityMapper());
	}

	@Override
	public T getById(I id) throws RepositoryException {
		T entity = findById(id);
		if (entity == null) {
			throw new RepositoryException("No entity " + getEntityClass() + " found of " + id);
		}
		return entity;
	}

	@Override
	public List<T> findByIds(Collection<I> ids) throws RepositoryException {
		StringBuilder sql = makeFindByIdSql();
		appendInClause(sql, ids);
		return findForList(sql.toString(), ids.toArray(), 0, 0, getEntityMapper());
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<T> findByIdsInOrder(Collection<I> ids) throws RepositoryException {
		List<T> list = findByIds(ids);
		if (list.isEmpty()) {
			return Collections.emptyList();
		}
		Map<I, T> map = new HashMap<>();
		for (T item : list) {
			map.put((I) getEntityMeta().getEntityId(item), item);
		}
		List<T> listInOrder = new ArrayList<>(list.size());
		for (I id : ids) {
			T item = map.get(id);
			if (item != null) {
				listInOrder.add(item);
			}
		}
		return listInOrder;
	}

	@Override
	public Map<I, T> findByIdsAsMap(Collection<I> ids) throws RepositoryException {
		return findForMapByIds(ids);
	}

	@Override
	public Map<I, T> findForMapByIds(Collection<I> ids) throws RepositoryException {
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

	private void doInsert(T entity) {
		EntityMeta meta = getEntityMeta();
		DefaultFieldConverter converter = getDefaultFieldConverter();
		String sql = meta.getInsertSql(entity, converter);
		SaveParams insertParams = meta.getInsertParams(Collections.singletonList(entity), converter);
		Class<?> generatedIdClass = meta.findGeneratedIdFieldType();
		if (generatedIdClass == null) {
			execute(sql, insertParams.getParams().get(0));
		} else {
			Object generatedId = getJdbcTemplate().execute(new GeneratKeysPreparedStatementCreator(sql),
					new InsertPreparedStatementCallback<>(insertParams.getParams().get(0), generatedIdClass));
			if (generatedId == null) {
				throw new RepositoryException("No id generated");
			}
			// 生成ID回传
			meta.setEntityId(entity, generatedId, converter);
		}
		// 版本号回传
		if (insertParams.versioning()) {
			meta.setVersionValue(entity, insertParams.getNewVersions().get(0));
		}
	}

	private boolean doUpdate(List<T> entityList, Set<String> fields, boolean upsert) {
		Object entity = entityList.get(0);
		EntityMeta meta = getEntityMeta();
		DefaultFieldConverter converter = getDefaultFieldConverter();
		String sql = meta.getUpdateSql(entity, fields, converter);
		SaveParams params = meta.getUpdateParams(entityList, fields, converter);
		int[] result;
		if (params.getParams().size() > 1) {
			result = executeBatch(sql, params.getParams());
		} else {
			result = new int[] { execute(sql, params.getParams().get(0)) };
		}
		boolean success = Arrays.stream(result).allMatch(i -> i > 0);
		boolean versioning = params.versioning();
		// 如果未能保存，如果版本控制，需要抛出相关异常，否则返回false
		if (!success) {
			if (versioning && !upsert) {
				throw versioningError(entityList, result);
			}
			return false;
		}
		// 保存成功，如果版本控制，版本号需要更新到实体
		if (versioning) {
			for (int i = 0; i < entityList.size(); i++) {
				meta.setVersionValue(entityList.get(i), params.getNewVersions().get(i));
			}
		}
		return true;
	}

	private StaleEntityRepositoryException versioningError(List<T> entityList, int[] result) {
		List<Object> badIds = new ArrayList<>(entityList.size());
		for (int i = 0; i < result.length; i++) {
			if (result[i] <= 0) {
				badIds.add(getEntityMeta().getEntityId(entityList.get(i)));
			}
		}
		return new StaleEntityRepositoryException(
				"Failed to update entity, maybe entity is stale: " + StringUtil.join(badIds));
	}

	private StringBuilder makeDeleteByIdSql() {
		StringBuilder buf = new StringBuilder("delete from ").append(getTable());
		appendWhere(buf).append(getEntityMeta().getIdColumnName());
		return buf;
	}

	private StringBuilder makeFindByIdSql() {
		StringBuilder buf = new StringBuilder("select * from ").append(getTable());
		appendWhere(buf).append(getEntityMeta().getIdColumnName());
		return buf;
	}

	private EntityMeta getEntityMeta() {
		if (entityMeta == null) {
			entityMeta = EntityManager.getEntityMetaOf(getEntityClass());
		}
		return entityMeta;
	}

	private static StringBuilder appendWhere(StringBuilder buf) {
		return buf.append(" where ");
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

}
