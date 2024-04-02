package com.sunnysuperman.repository.db;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.sunnysuperman.commons.util.StringUtil;
import com.sunnysuperman.repository.FieldValue;
import com.sunnysuperman.repository.InsertUpdate;
import com.sunnysuperman.repository.MultiColumn;
import com.sunnysuperman.repository.RepositoryException;
import com.sunnysuperman.repository.SerializeContext;
import com.sunnysuperman.repository.annotation.Column;
import com.sunnysuperman.repository.annotation.Entity;
import com.sunnysuperman.repository.annotation.Id;
import com.sunnysuperman.repository.annotation.IdStrategy;
import com.sunnysuperman.repository.annotation.Table;

class EntityMeta {
	protected List<EntityField> normalFields;
	protected EntityField idField;
	protected EntityField versionField;
	protected Id idInfo;
	protected String tableName;
	protected String insertSql;
	protected String updateSql;
	protected String emptyUpdateSql;
	protected Map<String, String> updateSqls = new ConcurrentHashMap<>();

	public static EntityMeta of(Class<?> clazz) {
		Entity entity = clazz.getAnnotation(Entity.class);
		if (entity == null) {
			throw new RepositoryException(clazz + " is not annotated with Entity");
		}
		Table table = clazz.getAnnotation(Table.class);
		if (table == null) {
			throw new RepositoryException(clazz + " is not annotated with Table");
		}
		Set<String> columnNames = new HashSet<>();

		EntityMeta meta = new EntityMeta();
		meta.tableName = table.name();
		meta.normalFields = new LinkedList<>();
		Stream.of(clazz.getMethods()).forEach(method -> {
			Field field = BeanUtils.getFieldByReadMethod(method);
			if (field == null) {
				return;
			}
			Column column = field.getAnnotation(Column.class);
			if (column == null) {
				return;
			}
			meta.addField(EntityField.of(field, method, column, table, columnNames), clazz);
		});
		meta.normalFields = Collections.unmodifiableList(new ArrayList<>(meta.normalFields));
		return meta;
	}

	public String getInsertSql(Object entity, DefaultFieldConverter defaultFieldConverter) {
		if (insertSql != null) {
			return insertSql;
		}
		SerializeContext context = new DBSerializeContext(entity, null, InsertUpdate.INSERT, defaultFieldConverter);
		List<String> insertColumns = new ArrayList<>(normalFields.size() + 1);
		// 特殊字段
		if (idField != null) {
			insertColumns.add(idField.columnName);
		}
		if (versionField != null) {
			insertColumns.add(versionField.columnName);
		}
		// 普通字段
		iterateNormalFields(false, null, new ColumnNameRetriever(insertColumns, context));
		// 组装SQL
		insertSql = new StringBuilder("insert into ").append(tableName).append('(')
				.append(StringUtil.join(insertColumns)).append(") values(")
				.append(StringUtil.join(insertColumns.stream().map(i -> "?").collect(Collectors.toList()))).append(')')
				.toString();
		return insertSql;
	}

	public SaveParams getInsertParams(List<?> entityList, DefaultFieldConverter defaultFieldConverter) {
		List<Object[]> paramsBatch = new ArrayList<>(entityList.size());
		List<Object> newVersions = versionField == null ? Collections.emptyList() : new ArrayList<>(entityList.size());
		entityList.forEach(entity -> {
			SerializeContext context = new DBSerializeContext(entity, null, InsertUpdate.INSERT, defaultFieldConverter);
			List<Object> params = new ArrayList<>(normalFields.size() + 1);
			if (idField != null) {
				params.add(idField.getColumnValue(entity, context));
			}
			if (versionField != null) {
				Object versionValue = versionField.getVersionValue(entity);
				if (versionValue == null) {
					versionValue = versionField.initVersionValue();
				}
				params.add(versionValue);
				newVersions.add(versionValue);
			}
			iterateNormalFields(false, null, new ColumnValueRetriever(params, context));
			paramsBatch.add(params.toArray());
		});
		return new SaveParams(paramsBatch, newVersions);
	}

	public String getUpdateSql(Object entity, Set<String> fields, DefaultFieldConverter defaultFieldConverter) {
		String sql;
		if (fields == null) {
			sql = updateSql;
		} else if (fields.isEmpty()) {
			sql = emptyUpdateSql;
		} else {
			sql = updateSqls.get(StringUtil.join(fields));
		}
		if (sql == null) {
			sql = makeUpdateSql(entity, fields, defaultFieldConverter);
			if (fields == null) {
				updateSql = sql;
			} else if (fields.isEmpty()) {
				emptyUpdateSql = sql;
			} else {
				updateSqls.put(StringUtil.join(fields), sql);
			}
		}
		return sql;
	}

	public SaveParams getUpdateParams(List<?> entityList, Set<String> fields,
			DefaultFieldConverter defaultFieldConverter) {
		List<Object[]> paramsBatch = new ArrayList<>(entityList.size());
		List<Object> newVersions = versionField == null ? Collections.emptyList() : new ArrayList<>(entityList.size());
		entityList.forEach(entity -> {
			SerializeContext context = new DBSerializeContext(entity, fields, InsertUpdate.INSERT,
					defaultFieldConverter);
			List<Object> params = new ArrayList<>(normalFields.size() + 1);
			iterateNormalFields(true, fields, new ColumnValueRetriever(params, context));
			// 更新后的版本号
			if (versionField != null) {
				Object newVersion = versionField.getNextVersionValue(entity);
				params.add(newVersion);
				newVersions.add(newVersion);
			}
			Object id = idField == null ? null : idField.getColumnValue(entity, context);
			if (id == null) {
				throw new RepositoryException("Require id to update");
			}
			params.add(id);
			if (versionField != null) {
				params.add(versionField.getVersionValue(entity));
			}
			paramsBatch.add(params.toArray());
		});
		return new SaveParams(paramsBatch, newVersions);
	}

	public String getIdColumnName() {
		return idField.columnName;
	}

	public Class<?> findIdFieldType() {
		return idField == null ? null : idField.getFieldType();
	}

	public Class<?> findGeneratedIdFieldType() {
		if (idInfo == null || idInfo.strategy() != IdStrategy.INCREMENT) {
			return null;
		}
		return findIdFieldType();
	}

	public Object setEntityId(Object entity, Object id, DefaultFieldConverter defaultFieldConverter) {
		try {
			return idField.setFieldValue(entity, id, null, defaultFieldConverter);
		} catch (RepositoryException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new RepositoryException(ex);
		}
	}

	public Object getEntityId(Object entity) {
		try {
			return idField.getRelationFieldValue(entity);
		} catch (RepositoryException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new RepositoryException(ex);
		}
	}

	public FieldValue findIdFieldAndValue(Object entity, DefaultFieldConverter defaultFieldConverter) {
		if (idField == null) {
			return null;
		}
		return makeFieldValue(idField, entity, defaultFieldConverter);
	}

	public FieldValue getIdFieldAndValue(Object entity, DefaultFieldConverter defaultFieldConverter) {
		FieldValue fieldValue = findIdFieldAndValue(entity, defaultFieldConverter);
		if (fieldValue == null) {
			throw new RepositoryException("No id field");
		}
		return fieldValue;
	}

	public FieldValue findVersionFieldAndValue(Object entity, DefaultFieldConverter defaultFieldConverter) {
		if (versionField == null) {
			return null;
		}
		return makeFieldValue(versionField, entity, defaultFieldConverter);
	}

	public FieldValue getVersionFieldAndValue(Object entity, DefaultFieldConverter defaultFieldConverter) {
		FieldValue fieldValue = findVersionFieldAndValue(entity, defaultFieldConverter);
		if (fieldValue == null) {
			throw new RepositoryException("No version field");
		}
		return fieldValue;
	}

	public void setVersionValue(Object entity, Object newVersion) {
		versionField.setFieldValue(entity, newVersion);
	}

	public Set<String> getColumnNames(Set<String> fields) {
		Set<String> columns = new HashSet<>(fields.size());
		for (EntityField f : normalFields) {
			if (fields.contains(f.fieldName)) {
				columns.add(f.columnName);
			}
		}
		if (idField != null && fields.contains(idField.fieldName)) {
			columns.add(idField.columnName);
		}
		return columns;
	}

	private String makeUpdateSql(Object entity, Set<String> fields, DefaultFieldConverter defaultFieldConverter) {
		SerializeContext context = new DBSerializeContext(entity, fields, InsertUpdate.UPDATE, defaultFieldConverter);
		// 更新字段
		List<String> updateColumns = new ArrayList<>(normalFields.size());
		if (fields == null || !fields.isEmpty()) {
			iterateNormalFields(true, fields, new ColumnNameRetriever(updateColumns, context));
		}
		if (versionField != null) {
			updateColumns.add(versionField.columnName);
		}
		if (updateColumns.isEmpty()) {
			throw new RepositoryException("No columns to update for " + tableName);
		}
		// 条件字段
		List<String> conditionalColumns = new ArrayList<>(2);
		if (idField != null) {
			conditionalColumns.add(idField.columnName);
		}
		if (versionField != null) {
			conditionalColumns.add(versionField.columnName);
		}
		// 组装SQL
		StringBuilder b = new StringBuilder("update ").append(tableName).append(" set ");
		b.append(StringUtil.join(updateColumns.stream().map(i -> i + "=?").collect(Collectors.toList())));
		b.append(" where ");
		b.append(StringUtil.join(conditionalColumns.stream().map(i -> i + "=?").collect(Collectors.toList()), " and "));
		return b.toString();
	}

	private void addField(EntityField f, Class<?> clazz) {
		Field field = f.field;
		if (field.getAnnotation(Id.class) != null) {
			if (idField != null) {
				throw new RepositoryException("Duplicate id field of " + clazz);
			}
			idField = f;
			idInfo = field.getAnnotation(Id.class);
		} else {
			normalFields.add(f);
			if (f.isValidVersionField()) {
				if (versionField != null) {
					throw new RepositoryException("Duplicate version field " + field + " of " + clazz);
				}
				versionField = f;
			}
		}
	}

	private static interface FieldHandler {

		void handle(EntityField field);

	}

	private static class ColumnNameRetriever implements FieldHandler {
		List<String> columns;
		SerializeContext context;

		public ColumnNameRetriever(List<String> columns, SerializeContext context) {
			super();
			this.columns = columns;
			this.context = context;
		}

		@Override
		public void handle(EntityField field) {
			Object columnValue = field.getColumnValue(context.getEntity(), context);
			if (columnValue instanceof MultiColumn) {
				MultiColumn mc = (MultiColumn) columnValue;
				Map<String, Object> cols = mc.getColumns();
				cols.entrySet().forEach(entry -> columns.add(entry.getKey()));
			} else {
				columns.add(field.columnName);
			}
		}

	}

	private static class ColumnValueRetriever implements FieldHandler {
		List<Object> params;
		SerializeContext context;

		public ColumnValueRetriever(List<Object> params, SerializeContext context) {
			super();
			this.params = params;
			this.context = context;
		}

		@Override
		public void handle(EntityField field) {
			Object columnValue = field.getColumnValue(context.getEntity(), context);
			if (columnValue instanceof MultiColumn) {
				MultiColumn mc = (MultiColumn) columnValue;
				Map<String, Object> cols = mc.getColumns();
				cols.entrySet().forEach(entry -> params.add(entry.getValue()));
			} else {
				params.add(field.getColumnValue(context.getEntity(), context));
			}
		}

	}

	private void iterateNormalFields(boolean update, Set<String> fields, FieldHandler fieldHandler) {
		normalFields.forEach(field -> {
			// 版本号单独处理
			if (field == versionField) {
				return;
			}
			Column column = field.column;
			if (fields != null) {
				// 指定字段插入或更新
				if (!fields.contains(field.fieldName)) {
					return;
				}
			} else {
				boolean toSave = update ? column.updatable() : column.insertable();
				if (!toSave) {
					return;
				}
			}
			fieldHandler.handle(field);
		});
	}

	private static FieldValue makeFieldValue(EntityField eField, Object entity,
			DefaultFieldConverter defaultFieldConverter) {
		FieldValue fieldValue = new FieldValue();
		fieldValue.setName(eField.fieldName);
		fieldValue.setValue(eField.getRelationFieldValue(entity));
		fieldValue.setColumnName(eField.columnName);
		fieldValue.setColumnValue(eField.getColumnValue(entity,
				new DBSerializeContext(entity, null, InsertUpdate.UPSERT, defaultFieldConverter)));
		return fieldValue;
	}
}