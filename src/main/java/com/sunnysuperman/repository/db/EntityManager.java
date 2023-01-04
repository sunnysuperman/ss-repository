package com.sunnysuperman.repository.db;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sunnysuperman.commons.util.StringUtil;
import com.sunnysuperman.repository.ClassFinder;
import com.sunnysuperman.repository.ClassFinder.ClassFilter;
import com.sunnysuperman.repository.FieldConverter;
import com.sunnysuperman.repository.FieldValue;
import com.sunnysuperman.repository.InsertUpdate;
import com.sunnysuperman.repository.MultiColumn;
import com.sunnysuperman.repository.RepositoryException;
import com.sunnysuperman.repository.SerializeContext;
import com.sunnysuperman.repository.annotation.Column;
import com.sunnysuperman.repository.annotation.Entity;
import com.sunnysuperman.repository.annotation.Id;
import com.sunnysuperman.repository.annotation.IdStrategy;
import com.sunnysuperman.repository.annotation.ManyToOne;
import com.sunnysuperman.repository.annotation.OneToOne;
import com.sunnysuperman.repository.annotation.Table;
import com.sunnysuperman.repository.annotation.VersionControl;

public class EntityManager {
	private static final Logger LOG = LoggerFactory.getLogger(EntityManager.class);
	private static Map<Class<?>, EntityMeta> META_MAP = new ConcurrentHashMap<>();

	private static class EntityClassFilter implements ClassFilter {

		@Override
		public boolean filter(Class<?> clazz) {
			return clazz.isAnnotationPresent(Entity.class);
		}

	}

	protected static class EntityMeta {
		protected List<EntityField> normalFields;
		protected EntityField idField;
		protected EntityField versionField;
		protected Id idInfo;
		protected String tableName;

		public Set<String> columnNames(Set<String> fields) {
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
	}

	protected static class EntityField {
		protected String fieldName;
		protected String columnName;
		protected Field field;
		protected Method readMethod;
		protected Method writeMethod;
		protected Column column;
		protected boolean relation;
		protected String relationFieldName;
		protected EntityField relationField;
		@SuppressWarnings("rawtypes")
		protected FieldConverter cachedConverter;

		@SuppressWarnings("rawtypes")
		private FieldConverter getConverter() {
			if (cachedConverter != null) {
				return cachedConverter;
			}
			Class<?> converterClass = column.converter();
			if (converterClass == void.class) {
				return null;
			}
			FieldConverter converter;
			try {
				converter = (FieldConverter) converterClass.newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				throw new RepositoryException(e);
			}
			if (column.converterCache()) {
				cachedConverter = converter;
			}
			return converter;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		public Object getColumnValue(Object entity, SerializeContext context,
				DefaultFieldConverter defaultFieldConverter) {
			Object fieldValue = getFieldValue(entity);
			// 自定义转换器
			FieldConverter converter = getConverter();
			if (converter != null) {
				return converter.convertToColumn(fieldValue, context);
			}
			if (fieldValue == null) {
				return null;
			}
			// 默认转换器
			if (!relation) {
				return defaultFieldConverter.convertToColumn(fieldValue);
			}
			// 关联对象
			return ensureRelationField().getColumnValue(fieldValue, context, defaultFieldConverter);
		}

		public Object getFieldValue(Object entity) {
			try {
				return readMethod.invoke(entity);
			} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
				throw new RepositoryException(e);
			}
		}

		public Object getRelationFieldValue(Object entity) {
			if (!relation) {
				return getFieldValue(entity);
			}
			return ensureRelationField().getRelationFieldValue(getFieldValue(entity));
		}

		@SuppressWarnings({ "rawtypes" })
		public Object setFieldValue(Object entity, Object value, DBDeserializeContext context,
				DefaultFieldConverter defaultFieldConverter) throws Exception {
			// 1.自定义转换器
			FieldConverter converter = getConverter();
			if (converter != null) {
				@SuppressWarnings("unchecked")
				Object convertedValue = converter.convertToField(value, field.getType(), context);
				writeMethod.invoke(entity, convertedValue);
				return convertedValue;
			}
			// 2.null
			if (value == null) {
				return null;
			}
			// 3.默认转换器
			Class<?> type = field.getType();
			if (!relation) {
				// 3.1非关联对象
				if (value.getClass() != type) {
					value = defaultFieldConverter.convertToField(value, type, field.getGenericType());
				}
				try {
					writeMethod.invoke(entity, value);
				} catch (Exception ex) {
					throw new RuntimeException("Failed to set value of " + field.getName() + " by value: ["
							+ value.getClass() + "] " + value.toString(), ex);
				}
				return value;
			}
			// 3.2关联对象
			Object relationEntity = type.newInstance();
			writeMethod.invoke(entity, relationEntity);
			ensureRelationField().setFieldValue(relationEntity, value, context, defaultFieldConverter);
			return relationEntity;
		}

		private EntityField ensureRelationField() {
			if (relationField != null) {
				return relationField;
			}
			EntityMeta meta = getEntityMetaOf(field.getType());
			if (meta == null) {
				throw new RepositoryException(field.getType() + " is not registered");
			}
			if (relationFieldName == null) {
				relationField = meta.idField;
			} else {
				for (EntityField field : meta.normalFields) {
					if (relationFieldName.equals(field.fieldName)) {
						relationField = field;
						break;
					}
				}
			}
			if (relationField == null) {
				throw new RepositoryException(field + " is not related to a valid entity");
			}
			return relationField;
		}
	}

	private static EntityMeta makeSerializeMeta(Class<?> clazz, Entity entity) throws Exception {
		if (entity == null) {
			throw new RepositoryException(clazz + " is not annotated with Entity");
		}
		EntityMeta meta = new EntityMeta();
		Table table = clazz.getAnnotation(Table.class);
		if (table == null) {
			throw new RepositoryException(clazz + " is not annotated with Table");
		}
		meta.tableName = table.name();
		Set<String> columnNames = new HashSet<>();
		List<EntityField> normalFields = new LinkedList<>();
		EntityField idField = null;
		Id idInfo = null;
		for (Method method : clazz.getMethods()) {
			Field field = BeanUtils.getFieldByReadMethod(method);
			if (field == null) {
				continue;
			}
			Column column = field.getAnnotation(Column.class);
			if (column == null) {
				// not a column
				continue;
			}
			String columnName = column.name();
			if (columnName == null || columnName.isEmpty()) {
				columnName = field.getName();
			}
			columnName = table.mapCamelToUnderscore() ? StringUtil.camel2underscore(columnName) : columnName;
			if (columnNames.contains(columnName)) {
				throw new RepositoryException("Duplicated column '" + columnName + "' in " + clazz);
			}
			columnNames.add(columnName);
			EntityField f = new EntityField();
			f.field = field;
			f.readMethod = method;
			f.writeMethod = BeanUtils.getWriteMethodByField(field);
			f.fieldName = field.getName();
			f.columnName = columnName;
			f.column = column;
			OneToOne oneToOne = field.getAnnotation(OneToOne.class);
			ManyToOne manyToOne = oneToOne == null ? field.getAnnotation(ManyToOne.class) : null;
			if (oneToOne != null || manyToOne != null) {
				Class<?> relationType = field.getType();
				if (relationType.getAnnotation(Entity.class) == null) {
					throw new RepositoryException(relationType + " is not annotated with Entity");
				}
				f.relation = true;
				f.relationFieldName = StringUtil
						.emptyToNull(oneToOne != null ? oneToOne.relationField() : manyToOne.relationField());

			}
			if (field != null && field.getAnnotation(Id.class) != null) {
				if (idField != null) {
					throw new RepositoryException("Duplicate id column of " + clazz);
				}
				idField = f;
				idInfo = field.getAnnotation(Id.class);
			} else {
				normalFields.add(f);
				if (field.getAnnotation(VersionControl.class) != null) {
					if (meta.versionField != null) {
						throw new RepositoryException("Duplicate version column of " + clazz);
					}
					if (!f.column.updatable()) {
						throw new RepositoryException("Version column should be updatable of " + clazz);
					}
					meta.versionField = f;
				}
			}
		}
		// to save memory
		meta.normalFields = new ArrayList<>(normalFields);
		normalFields = null;
		meta.idField = idField;
		meta.idInfo = idInfo;
		return meta;
	}

	public static void scan(String packageName) throws Exception {
		long t1 = System.nanoTime();
		Set<Class<?>> classes = ClassFinder.find(packageName, new EntityClassFilter());
		for (Class<?> clazz : classes) {
			loadEntity(clazz);
		}
		if (LOG.isInfoEnabled()) {
			long t2 = System.nanoTime();
			LOG.info("Entity scanning for package {} took {}ms, {} entities found", packageName,
					TimeUnit.NANOSECONDS.toMillis(t2 - t1), classes.size());
		}
	}

	private static EntityMeta loadEntity(Class<?> clazz) throws Exception {
		Entity entity = clazz.getAnnotation(Entity.class);
		EntityMeta meta = makeSerializeMeta(clazz, entity);
		META_MAP.put(clazz, meta);
		return meta;
	}

	private static EntityMeta getEntityMetaOf(Class<?> clazz) {
		EntityMeta meta = META_MAP.get(clazz);
		if (meta == null) {
			LOG.warn("Lazy load entity {}", clazz);
			try {
				meta = loadEntity(clazz);
			} catch (RepositoryException e) {
				throw e;
			} catch (Exception e) {
				throw new RepositoryException(e);
			}
		}
		return meta;
	}

	public static String getTable(Class<?> clazz) {
		return clazz.getAnnotation(Table.class).name();
	}

	public static SerializedRow serialize(Object entity, InsertUpdate insertUpdate,
			DefaultFieldConverter defaultFieldConverter) throws RepositoryException {
		return serialize(entity, null, insertUpdate, defaultFieldConverter);
	}

	public static SerializedRow serialize(Object entity, Set<String> fields, InsertUpdate insertUpdate,
			DefaultFieldConverter defaultFieldConverter) throws RepositoryException {
		EntityMeta meta = getEntityMetaOf(entity.getClass());
		SerializeContext context = new DBSerializeContext(entity, fields, insertUpdate, defaultFieldConverter);
		SerializedRow row = new SerializedRow();
		row.setTableName(meta.tableName);
		Object id = null;
		boolean update = false;
		if (meta.idField != null) {
			id = meta.idField.getColumnValue(entity, context, defaultFieldConverter);
			switch (insertUpdate) {
			case INSERT:
				update = false;
				break;
			case UPDATE:
				update = true;
				break;
			case UPSERT:
				update = id != null;
				break;
			default:
				throw new RepositoryException("Unknown InsertUpdate");
			}
		}
		boolean upsert = false;
		if (update) {
			if (id == null) {
				throw new RepositoryException("Require id to update");
			}
			if (meta.versionField == null) {
				row.setIdColumns(new String[] { meta.idField.columnName });
				row.setIdValues(new Object[] { id });
			} else {
				Object version = meta.versionField.getColumnValue(entity, context, defaultFieldConverter);
				if (version == null) {
					throw new RepositoryException("Version is null on update");
				}
				row.setIdColumns(new String[] { meta.idField.columnName, meta.versionField.columnName });
				row.setIdValues(new Object[] { id, version });
			}
			if (insertUpdate == InsertUpdate.UPSERT && meta.idInfo.strategy() == IdStrategy.PROVIDED
					&& meta.versionField == null) {
				upsert = true;
			}
		} else {
			Id idInfo = meta.idInfo;
			if (idInfo != null && idInfo.strategy() == IdStrategy.INCREMENT) {
				row.setIdGeneration(true);
			}
		}
		Map<String, Object> doc = new HashMap<>();
		row.setData(doc);
		Map<String, Object> upsertDoc = null;
		if (upsert) {
			upsertDoc = new HashMap<>();
			if (id != null) {
				upsertDoc.put(meta.idField.columnName, id);
			}
			row.setUpsertData(upsertDoc);
		}
		for (EntityField field : meta.normalFields) {
			Column column = field.column;
			Object columnValue = null;
			boolean columnValueGot = false;
			if (fields != null) {
				// 指定字段插入或更新
				if (!fields.contains(field.fieldName)) {
					continue;
				}
			} else {
				// 更新不成插入
				if (upsert) {
					if (column.insertable()) {
						if (!columnValueGot) {
							columnValue = field.getColumnValue(entity, context, defaultFieldConverter);
							columnValueGot = true;
						}
						setColumn(upsertDoc, field, columnValue);
					}
				}
				// 仅更新
				if (update) {
					if (!column.updatable()) {
						continue;
					}
				}
				// 仅插入
				else if (!column.insertable()) {
					continue;
				}
			}
			if (update && field == meta.versionField) {
				doc.put("$inc", Collections.singletonMap(field.columnName, 1));
			} else {
				columnValue = columnValueGot ? columnValue
						: field.getColumnValue(entity, context, defaultFieldConverter);
				setColumn(doc, field, columnValue);
			}
		}
		if (!update && id != null) {
			doc.put(meta.idField.columnName, id);
		}
		return row;
	}

	private static void setColumn(Map<String, Object> doc, EntityField field, Object columnValue) {
		if (columnValue != null && columnValue instanceof MultiColumn) {
			MultiColumn mc = (MultiColumn) columnValue;
			Map<String, Object> cols = mc.getColumns();
			if (cols != null) {
				doc.putAll(cols);
			}
		} else {
			doc.put(field.columnName, columnValue);
		}
	}

	public static <T> T deserialize(Map<String, Object> doc, Class<T> type, DefaultFieldConverter defaultFieldConverter)
			throws RepositoryException {
		EntityMeta meta = getEntityMetaOf(type);
		DBDeserializeContext context = new DBDeserializeContext(doc, defaultFieldConverter);
		try {
			T entity = type.newInstance();
			for (EntityField field : meta.normalFields) {
				field.setFieldValue(entity, doc.get(field.columnName), context, defaultFieldConverter);
			}
			if (meta.idField != null) {
				meta.idField.setFieldValue(entity, doc.get(meta.idField.columnName), context, defaultFieldConverter);
			}
			return entity;
		} catch (RepositoryException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new RepositoryException(ex);
		}
	}

	public static String getIdColumnName(Class<?> clazz) {
		EntityMeta meta = getEntityMetaOf(clazz);
		return meta.idField.columnName;
	}

	public static Object setEntityId(Object entity, Object id, DefaultFieldConverter defaultFieldConverter)
			throws RepositoryException {
		EntityMeta meta = getEntityMetaOf(entity.getClass());
		try {
			return meta.idField.setFieldValue(entity, id, null, defaultFieldConverter);
		} catch (RepositoryException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new RepositoryException(ex);
		}
	}

	public static Object getEntityId(Object entity) throws RepositoryException {
		EntityMeta meta = getEntityMetaOf(entity.getClass());
		try {
			return meta.idField.getRelationFieldValue(entity);
		} catch (RepositoryException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new RepositoryException(ex);
		}
	}

	public static FieldValue findIdFieldAndValue(Object entity, DefaultFieldConverter defaultFieldConverter)
			throws RepositoryException {
		EntityMeta meta = getEntityMetaOf(entity.getClass());
		if (meta.idField == null) {
			return null;
		}
		return makeFieldValue(meta.idField, entity, defaultFieldConverter);
	}

	public static FieldValue getIdFieldAndValue(Object entity, DefaultFieldConverter defaultFieldConverter)
			throws RepositoryException {
		FieldValue fieldValue = findIdFieldAndValue(entity, defaultFieldConverter);
		if (fieldValue == null) {
			throw new RepositoryException("No id field");
		}
		return fieldValue;
	}

	public static FieldValue findVersionFieldAndValue(Object entity, DefaultFieldConverter defaultFieldConverter)
			throws RepositoryException {
		EntityMeta meta = getEntityMetaOf(entity.getClass());
		if (meta.versionField == null) {
			return null;
		}
		return makeFieldValue(meta.versionField, entity, defaultFieldConverter);
	}

	public static FieldValue getVersionFieldAndValue(Object entity, DefaultFieldConverter defaultFieldConverter)
			throws RepositoryException {
		FieldValue fieldValue = findVersionFieldAndValue(entity, defaultFieldConverter);
		if (fieldValue == null) {
			throw new RepositoryException("No version field");
		}
		return fieldValue;
	}

	public static Set<String> findColumnNames(Class<?> clazz, Set<String> fields) {
		EntityMeta meta = getEntityMetaOf(clazz);
		return meta.columnNames(fields);
	}

	private static FieldValue makeFieldValue(EntityField eField, Object entity,
			DefaultFieldConverter defaultFieldConverter) {
		FieldValue fieldValue = new FieldValue();
		fieldValue.setName(eField.fieldName);
		fieldValue.setValue(eField.getRelationFieldValue(entity));
		fieldValue.setColumnName(eField.columnName);
		fieldValue.setColumnValue(eField.getColumnValue(entity, null, defaultFieldConverter));
		return fieldValue;
	}
}
