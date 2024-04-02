package com.sunnysuperman.repository.db;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sunnysuperman.commons.util.TypeFinder;
import com.sunnysuperman.repository.RepositoryException;
import com.sunnysuperman.repository.annotation.Entity;
import com.sunnysuperman.repository.annotation.Table;

public class EntityManager {
	private static final Logger LOG = LoggerFactory.getLogger(EntityManager.class);
	private static Map<Class<?>, EntityMeta> metaMap = new ConcurrentHashMap<>();

	private EntityManager() {
		// nope
	}

	public static void scan(String packageName) {
		scan(new String[] { packageName });
	}

	public static void scan(String[] packageNames) {
		long t1 = System.nanoTime();
		Set<Class<?>> classes = TypeFinder.findTypesAnnotatedWith(packageNames, Entity.class);
		for (Class<?> clazz : classes) {
			loadEntityMeta(clazz);
		}
		if (LOG.isInfoEnabled()) {
			long t2 = System.nanoTime();
			LOG.info("Entity scanning for package {} took {}ms, {} entities found, {}", packageNames,
					TimeUnit.NANOSECONDS.toMillis(t2 - t1), classes.size(), classes);
		}
	}

	public static String getTable(Class<?> clazz) {
		return clazz.getAnnotation(Table.class).name();
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

	static EntityMeta getEntityMetaOf(Class<?> clazz) {
		EntityMeta meta = metaMap.get(clazz);
		if (meta == null) {
			LOG.warn("Lazy load entity {}", clazz);
			try {
				meta = loadEntityMeta(clazz);
			} catch (RepositoryException e) {
				throw e;
			} catch (Exception e) {
				throw new RepositoryException(e);
			}
		}
		return meta;
	}

	private static EntityMeta loadEntityMeta(Class<?> clazz) {
		EntityMeta meta = EntityMeta.of(clazz);
		metaMap.put(clazz, meta);
		return meta;
	}

}
