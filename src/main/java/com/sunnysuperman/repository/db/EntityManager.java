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

import com.sunnysuperman.commons.util.StringUtil;
import com.sunnysuperman.repository.ClassFinder;
import com.sunnysuperman.repository.ClassFinder.ClassFilter;
import com.sunnysuperman.repository.FieldConverter;
import com.sunnysuperman.repository.InsertUpdate;
import com.sunnysuperman.repository.RepositoryException;
import com.sunnysuperman.repository.annotation.Column;
import com.sunnysuperman.repository.annotation.Entity;
import com.sunnysuperman.repository.annotation.Id;
import com.sunnysuperman.repository.annotation.IdStrategy;
import com.sunnysuperman.repository.annotation.ManyToOne;
import com.sunnysuperman.repository.annotation.OneToOne;
import com.sunnysuperman.repository.annotation.Table;

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
		protected Id idInfo;
		protected String tableName;
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
		public Object getColumnValue(Object entity, DefautFieldConverter defaultFieldConverter) {
			Object fieldValue = getFieldValue(entity);
			// 自定义转换器
			FieldConverter converter = getConverter();
			if (converter != null) {
				return converter.convertToColumn(fieldValue);
			}
			if (fieldValue == null) {
				return null;
			}
			// 默认转换器
			if (!relation) {
				return defaultFieldConverter.convertToColumn(fieldValue);
			}
			// 关联对象
			return ensureRelationField().getColumnValue(fieldValue, defaultFieldConverter);
		}

		public Object getFieldValue(Object entity) {
			try {
				return readMethod.invoke(entity);
			} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
				throw new RepositoryException(e);
			}
		}

		@SuppressWarnings({ "rawtypes" })
		public Object setFieldValue(Object entity, Object value, DefautFieldConverter defaultFieldConverter)
				throws Exception {
			// 1.自定义转换器
			FieldConverter converter = getConverter();
			if (converter != null) {
				@SuppressWarnings("unchecked")
				Object convertedValue = converter.convertToField(value, field.getType());
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
				writeMethod.invoke(entity, value);
				return value;
			}
			// 3.2关联对象
			Object relationEntity = type.newInstance();
			writeMethod.invoke(entity, relationEntity);
			ensureRelationField().setFieldValue(relationEntity, value, defaultFieldConverter);
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
			EntityField efield = new EntityField();
			efield.field = field;
			efield.readMethod = method;
			efield.writeMethod = BeanUtils.getWriteMethodByField(field);
			efield.fieldName = field.getName();
			efield.columnName = columnName;
			efield.column = column;
			OneToOne oneToOne = field.getAnnotation(OneToOne.class);
			ManyToOne manyToOne = oneToOne == null ? field.getAnnotation(ManyToOne.class) : null;
			if (oneToOne != null || manyToOne != null) {
				Class<?> relationType = field.getType();
				if (relationType.getAnnotation(Entity.class) == null) {
					throw new RepositoryException(relationType + " is not annotated with Entity");
				}
				efield.relation = true;
				efield.relationFieldName = StringUtil
						.emptyToNull(oneToOne != null ? oneToOne.relationField() : manyToOne.relationField());

			}
			if (field != null && field.getAnnotation(Id.class) != null) {
				if (idField != null) {
					throw new RepositoryException("Duplicated id column of " + clazz);
				}
				idField = efield;
				idInfo = field.getAnnotation(Id.class);
			} else {
				normalFields.add(efield);
			}
		}
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
			DefautFieldConverter defaultFieldConverter) throws RepositoryException {
		return serialize(entity, null, insertUpdate, defaultFieldConverter);
	}

	public static SerializedRow serialize(Object entity, Set<String> fields, InsertUpdate insertUpdate,
			DefautFieldConverter defaultFieldConverter) throws RepositoryException {
		EntityMeta meta = getEntityMetaOf(entity.getClass());
		SerializedRow row = new SerializedRow();
		row.setTableName(meta.tableName);
		Object id = null;
		boolean update = false;
		if (meta.idField != null) {
			row.setIdColumns(new String[] { meta.idField.columnName });
			id = meta.idField.getColumnValue(entity, defaultFieldConverter);
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
			row.setIdValues(new Object[] { id });
			if (insertUpdate == InsertUpdate.UPSERT) {
				upsert = meta.idInfo.strategy() == IdStrategy.PROVIDED;
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
				upsertDoc.put(row.getIdColumns()[0], id);
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
				// 所有字段插入或更新
				if (upsert) {
					if (column.insertable()) {
						columnValue = field.getColumnValue(entity, defaultFieldConverter);
						columnValueGot = true;
						upsertDoc.put(field.columnName, columnValue);
					}
				}
				if (update) {
					if (!column.updatable()) {
						continue;
					}
				} else if (!column.insertable()) {
					continue;
				}
			}
			doc.put(field.columnName,
					columnValueGot ? columnValue : field.getColumnValue(entity, defaultFieldConverter));
		}
		if (!update && id != null) {
			doc.put(row.getIdColumns()[0], id);
		}
		return row;
	}

	public static <T> T deserialize(Map<String, Object> doc, Class<T> type, DefautFieldConverter defaultFieldConverter)
			throws RepositoryException {
		EntityMeta meta = getEntityMetaOf(type);
		try {
			T entity = type.newInstance();
			for (EntityField field : meta.normalFields) {
				field.setFieldValue(entity, doc.get(field.columnName), defaultFieldConverter);
			}
			if (meta.idField != null) {
				meta.idField.setFieldValue(entity, doc.get(meta.idField.columnName), defaultFieldConverter);
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

	public static Object setEntityId(Object entity, Object id, DefautFieldConverter defaultFieldConverter)
			throws RepositoryException {
		EntityMeta meta = getEntityMetaOf(entity.getClass());
		try {
			return meta.idField.setFieldValue(entity, id, defaultFieldConverter);
		} catch (RepositoryException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new RepositoryException(ex);
		}
	}

	public static Object getEntityId(Object entity) throws RepositoryException {
		EntityMeta meta = getEntityMetaOf(entity.getClass());
		try {
			return meta.idField.getFieldValue(entity);
		} catch (RepositoryException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new RepositoryException(ex);
		}
	}
}
