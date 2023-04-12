package com.sunnysuperman.repository.db;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
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

import com.sunnysuperman.commons.util.FormatUtil;
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
	private static final Long VERSION_LONG_1 = 1L;
	private static final Integer VERSION_INT_1 = 1;
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

		public Object initVersionValue() {
			Class<?> type = getFieldType();
			if (type == long.class || type == Long.class) {
				return VERSION_LONG_1;
			}
			if (type == int.class || type == Integer.class) {
				return VERSION_INT_1;
			}
			throw new RuntimeException("Failed to initVersionValue for " + type);
		}

		public Object makeNextVersionValue(Object entity) {
			Object fieldValue = getFieldValue(entity);
			if (fieldValue == null) {
				return initVersionValue();
			}
			Class<?> type = fieldValue.getClass();
			if (type == long.class || type == Long.class) {
				return Long.valueOf(FormatUtil.parseLongValue(fieldValue, 0) + 1);
			}
			if (type == int.class || type == Integer.class) {
				return Integer.valueOf(FormatUtil.parseIntValue(fieldValue, 0) + 1);
			}
			throw new RuntimeException("Failed to makeNextVersionValue for " + type);
		}

		public Class<?> getFieldType() {
			if (!relation) {
				return field.getType();
			}
			return ensureRelationField().getFieldType();
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

		public void setFieldValue(Object entity, Object value) {
			try {
				writeMethod.invoke(entity, value);
			} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
				throw new RepositoryException(e);
			}
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
					throw new RepositoryException("Duplicate id field of " + clazz);
				}
				idField = f;
				idInfo = field.getAnnotation(Id.class);
			} else {
				normalFields.add(f);
				if (field.getAnnotation(VersionControl.class) != null) {
					if (meta.versionField != null) {
						throw new RepositoryException("Duplicate version field: " + field);
					}
					if (field.getType() != Integer.class && field.getType() != Long.class) {
						throw new RepositoryException("Version field should be Integer or Long: " + field);
					}
					if (!f.column.insertable() || !f.column.updatable()) {
						throw new RepositoryException("Version field should be insertable/updatable: " + field);
					}
					if (f.getConverter() != null) {
						throw new RepositoryException("Version field should not have a converter: " + field);
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
		row.setVersioning(meta.versionField != null);
		Object id = meta.idField == null ? null : meta.idField.getColumnValue(entity, context, defaultFieldConverter);
		boolean update = (insertUpdate == InsertUpdate.UPDATE) || (insertUpdate == InsertUpdate.UPSERT && id != null);
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
		} else {
			Id idInfo = meta.idInfo;
			if (idInfo != null && idInfo.strategy() == IdStrategy.INCREMENT) {
				row.setIdGeneration(true);
			}
		}
		Map<String, Object> doc = new HashMap<>();
		// 插入或更新数据
		row.setData(doc);
		// 动态更新或者插入
		boolean upsert = insertUpdate == InsertUpdate.UPSERT && meta.idInfo.strategy() == IdStrategy.PROVIDED
				&& (id != null);
		Map<String, Object> upsertDoc = null;
		if (upsert) {
			upsertDoc = new HashMap<>();
			if (id != null) {
				upsertDoc.put(meta.idField.columnName, id);
			}
			row.setUpsertData(upsertDoc);
		}
		for (EntityField field : meta.normalFields) {
			// 版本号单独处理
			if (field == meta.versionField) {
				continue;
			}
			Column column = field.column;
			boolean columnToSave;
			if (fields != null) {
				// 指定字段插入或更新
				if (!fields.contains(field.fieldName)) {
					continue;
				}
				columnToSave = true;
			} else {
				if (update) {
					columnToSave = column.updatable();
				} else {
					columnToSave = column.insertable();
				}
			}
			boolean columnToUpsert = upsert && column.insertable();
			if (!columnToSave && !columnToUpsert) {
				continue;
			}
			// 普通字段写入
			Object columnValue = field.getColumnValue(entity, context, defaultFieldConverter);
			if (columnToSave) {
				setColumn(doc, field, columnValue);
			}
			if (columnToUpsert) {
				setColumn(upsertDoc, field, columnValue);
			}
		}
		if (!update && id != null) {
			doc.put(meta.idField.columnName, id);
		}
		// 版本字段写入
		if (meta.versionField != null) {
			EntityField field = meta.versionField;
			if (!update || upsert) {
				Object version = field.getFieldValue(entity);
				// 如果插入时不指定版本号，自动生成
				if (version == null) {
					version = field.initVersionValue();
					row.setInsertedVersion(version);
				}
				if (!update) {
					setColumn(doc, field, version);
				}
				if (upsert) {
					setColumn(upsertDoc, field, version);
				}
			}
			if (update) {
				// 更新版本号
				row.setUpdatedVersion(field.makeNextVersionValue(entity));
				setColumn(doc, field, row.getUpdatedVersion());
			}
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

	public static Class<?> getIdFieldType(Object entity) throws RepositoryException {
		EntityMeta meta = getEntityMetaOf(entity.getClass());
		return meta.idField == null ? null : meta.idField.getFieldType();
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

	public static void setVersionValue(Object entity, Object newVersion) throws RepositoryException {
		EntityMeta meta = getEntityMetaOf(entity.getClass());
		meta.versionField.setFieldValue(entity, newVersion);
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
