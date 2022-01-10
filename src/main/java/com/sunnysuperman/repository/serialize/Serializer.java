package com.sunnysuperman.repository.serialize;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.reflections.Reflections;

import com.sunnysuperman.commons.bean.Bean;
import com.sunnysuperman.commons.bean.ParseBeanOptions;
import com.sunnysuperman.commons.util.FormatUtil;
import com.sunnysuperman.commons.util.StringUtil;
import com.sunnysuperman.repository.InsertUpdate;
import com.sunnysuperman.repository.RepositoryException;

public class Serializer {

    protected static class SerializeMeta {
        protected List<SerializeField> normalFields;
        protected SerializeField idField;
        protected SerializeId idInfo;
        protected String tableName;
    }

    private static class SerializeField {
        private String fieldName;
        private String columnName;
        private Field field;
        private Method readMethod;
        private Method writeMethod;
        private Method relationWriteMethod;
        private Method relationReadMethod;
        private SerializeProperty property;

        public Object getRawValue(Object entity) {
            try {
                return readMethod.invoke(entity);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                throw new RepositoryException(e);
            }
        }

        public Object getValue(Object entity) {
            Object value = getRawValue(entity);
            if (value == null) {
                return null;
            }
            if (property.relation() == Relation.NONE) {
                return value;
            }
            try {
                return relationReadMethod.invoke(value);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                throw new RepositoryException(e);
            }
        }
    }

    private static Map<Class<?>, SerializeMeta> META_MAP = new HashMap<>();

    private static SerializeMeta getSerializeMeta(Class<?> clazz, SerializeBean sbeanInfo) throws Exception {
        SerializeMeta meta = new SerializeMeta();
        meta.tableName = sbeanInfo.value();
        Set<String> columnNames = new HashSet<>();
        List<SerializeField> normalFields = new LinkedList<>();
        SerializeField idField = null;
        SerializeId idInfo = null;
        for (Method method : clazz.getMethods()) {
            if (Modifier.isStatic(method.getModifiers())) {
                continue;
            }
            Class<?>[] paramTypes = method.getParameterTypes();
            if (paramTypes.length != 0) {
                continue;
            }
            String methodName = method.getName();
            int offset = 0;
            if (methodName.startsWith("get")) {
                offset = 3;
            } else if (methodName.startsWith("is")) {
                offset = 2;
            } else {
                continue;
            }
            final String fieldName = getFieldNameByMethod(methodName, offset);
            if (fieldName == null || fieldName.equals("class")) {
                continue;
            }
            Field field = null;
            try {
                field = method.getDeclaringClass().getDeclaredField(fieldName);
            } catch (NoSuchFieldException | SecurityException e) {
                // ignore
            }
            SerializeProperty property = method.getAnnotation(SerializeProperty.class);
            if (property == null && field != null) {
                property = field.getAnnotation(SerializeProperty.class);
            }
            if (property == null) {
                // not a serialize property
                continue;
            }
            String columnName = property.column();
            if (columnName == null || columnName.isEmpty()) {
                columnName = fieldName;
            }
            columnName = sbeanInfo.camel2underline() ? StringUtil.camel2underline(columnName) : columnName;
            if (columnNames.contains(columnName)) {
                throw new RepositoryException("Duplicated column '" + columnName + "' in " + clazz);
            }
            columnNames.add(columnName);
            SerializeField sfield = new SerializeField();
            sfield.field = field;
            sfield.readMethod = method;
            sfield.writeMethod = clazz.getMethod("set" + capitalize(fieldName), method.getReturnType());
            sfield.fieldName = fieldName;
            sfield.columnName = columnName;
            sfield.property = property;
            if (property.relation() != Relation.NONE) {
                sfield.relationWriteMethod = getRelationWriteMethod(method, property, columnName);
                sfield.relationReadMethod = getRelationReadMethod(method, property, columnName);
            }
            if (field != null && field.getAnnotation(SerializeId.class) != null) {
                if (idField != null) {
                    throw new RepositoryException("Duplicated id column of " + clazz);
                }
                idField = sfield;
                idInfo = field.getAnnotation(SerializeId.class);
            } else {
                normalFields.add(sfield);
            }
        }
        meta.normalFields = Collections.unmodifiableList(normalFields);
        meta.idField = idField;
        meta.idInfo = idInfo;
        return meta;
    }

    private static Method getRelationWriteMethod(Method readMethod, SerializeProperty property, String columnName)
            throws Exception {
        Class<?> relationClass = readMethod.getReturnType();
        if (relationClass.getAnnotation(SerializeBean.class) == null) {
            String idName = StringUtil.isNotEmpty(property.joinProperty()) ? property.joinProperty() : "id";
            Class<?> idType = relationClass.getMethod("get" + capitalize(idName)).getReturnType();
            return relationClass.getMethod("set" + capitalize(idName), idType);
        }
        Method method = getIdWriteMethod(relationClass);
        if (method == null) {
            throw new RepositoryException(
                    readMethod.getDeclaringClass() + "." + columnName + ": could not get relation write method");
        }
        return method;
    }

    private static Method getRelationReadMethod(Method readMethod, SerializeProperty property, String columnName)
            throws Exception {
        Class<?> relationClass = readMethod.getReturnType();
        if (relationClass.getAnnotation(SerializeBean.class) == null) {
            String idName = StringUtil.isNotEmpty(property.joinProperty()) ? property.joinProperty() : "id";
            return relationClass.getMethod("get" + capitalize(idName));
        }
        Method method = getIdReadMethod(relationClass);
        if (method == null) {
            throw new RepositoryException(
                    readMethod.getDeclaringClass() + "." + columnName + ": could not get relation read method");
        }
        return method;
    }

    private static Method getIdReadMethod(Class<?> relationClass) throws Exception {
        Field[] fields = relationClass.getDeclaredFields();
        for (Field field : fields) {
            if (field.getAnnotation(SerializeId.class) != null) {
                String idName = field.getName();
                return relationClass.getMethod("get" + capitalize(idName));
            }
        }
        Class<?> superClass = relationClass.getSuperclass();
        if (superClass == null || superClass == Object.class) {
            return null;
        }
        return getIdReadMethod(superClass);
    }

    private static Method getIdWriteMethod(Class<?> relationClass) throws Exception {
        Field[] fields = relationClass.getDeclaredFields();
        for (Field field : fields) {
            if (field.getAnnotation(SerializeId.class) != null) {
                String idName = field.getName();
                Class<?> idType = relationClass.getMethod("get" + capitalize(idName)).getReturnType();
                return relationClass.getMethod("set" + capitalize(idName), idType);
            }
        }
        Class<?> superClass = relationClass.getSuperclass();
        if (superClass == null || superClass == Object.class) {
            return null;
        }
        return getIdWriteMethod(superClass);
    }

    private static String getFieldNameByMethod(String methodName, int offset) {
        if (methodName.length() <= offset) {
            return null;
        }
        String field = methodName.substring(offset);
        if (field.length() == 1) {
            return field.toLowerCase();
        }
        return Character.toLowerCase(field.charAt(0)) + field.substring(1);
    }

    private static String capitalize(final String str) {
        if (str.length() > 1) {
            return Character.toUpperCase(str.charAt(0)) + str.substring(1);
        }
        return str.toUpperCase();
    }

    public static void scan(String packageName) throws Exception {
        Reflections reflections = packageName != null ? new Reflections(packageName) : new Reflections();
        Set<Class<?>> classes = reflections.getTypesAnnotatedWith(SerializeBean.class);
        for (Class<?> clazz : classes) {
            SerializeBean sbeanInfo = clazz.getAnnotation(SerializeBean.class);
            if (sbeanInfo == null) {
                continue;
            }
            META_MAP.put(clazz, getSerializeMeta(clazz, sbeanInfo));
        }
    }

    public static void setIncrementId(Object bean, Number id) {
        SerializeMeta meta = META_MAP.get(bean.getClass());
        Class<?> idClass = meta.idField.readMethod.getReturnType();
        Object convertedId = id;
        if (idClass != id.getClass()) {
            if (idClass.equals(String.class)) {
                convertedId = FormatUtil.parseString(id);
            } else if (idClass.equals(Integer.class)) {
                convertedId = FormatUtil.parseInteger(id);
            } else if (idClass.equals(Long.class)) {
                convertedId = FormatUtil.parseLong(id);
            } else {
                throw new RepositoryException("Failed to convert " + id.getClass() + " to " + idClass);
            }
        }
        try {
            meta.idField.writeMethod.invoke(bean, convertedId);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new RepositoryException(e);
        }
    }

    private static SerializeMeta getSerializeMeta(Class<?> clazz) {
        SerializeMeta meta = META_MAP.get(clazz);
        if (meta == null) {
            throw new RepositoryException(clazz + " is not annotated with SerializeBean");
        }
        return meta;
    }

    public static SerializeDoc serialize(Object bean, Set<String> fields, InsertUpdate insertUpdate)
            throws RepositoryException {
        SerializeMeta meta = getSerializeMeta(bean.getClass());
        SerializeDoc sdoc = new SerializeDoc();
        sdoc.setTableName(meta.tableName);
        Object id = null;
        boolean update = false;
        if (meta.idField != null) {
            sdoc.setIdColumns(new String[] { meta.idField.columnName });
            id = meta.idField.getValue(bean);
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
            sdoc.setIdValues(new Object[] { id });
            if (insertUpdate == InsertUpdate.UPSERT) {
                upsert = meta.idInfo.generator() == IdGenerator.PROVIDE;
            }
        } else {
            SerializeId idInfo = meta.idInfo;
            if (idInfo != null) {
                switch (idInfo.generator()) {
                case INCREMENT: {
                    Class<?> desiredClass = meta.idField.readMethod.getReturnType();
                    if (desiredClass == Long.class) {
                        sdoc.setIdIncrementClass(Long.class);
                    } else if (desiredClass == Integer.class) {
                        sdoc.setIdIncrementClass(Integer.class);
                    } else {
                        sdoc.setIdIncrementClass(idInfo.type());
                    }
                    break;
                }
                case PROVIDE:
                    break;
                default:
                    throw new RepositoryException("Unsupported id generator " + idInfo.generator());
                }
            }
        }
        Map<String, Object> doc = new HashMap<>();
        sdoc.setDoc(doc);
        Map<String, Object> upsertDoc = null;
        if (upsert) {
            upsertDoc = new HashMap<>();
            if (id != null) {
                upsertDoc.put(sdoc.getIdColumns()[0], id);
            }
            sdoc.setUpsertDoc(upsertDoc);
        }
        for (SerializeField sfield : meta.normalFields) {
            SerializeProperty property = sfield.property;
            if (fields != null) {
                // 指定字段插入或更新
                if (!fields.contains(sfield.fieldName)) {
                    continue;
                }
            } else {
                // 根据bean定义的字段插入或更新
                if (upsert) {
                    if (property.insertable()) {
                        upsertDoc.put(sfield.columnName, sfield.getValue(bean));
                    }
                }
                if (update) {
                    if (!property.updatable()) {
                        continue;
                    }
                } else if (!property.insertable()) {
                    continue;
                }
            }
            doc.put(sfield.columnName, sfield.getValue(bean));
        }
        if (!update && id != null) {
            doc.put(sdoc.getIdColumns()[0], id);
        }
        return sdoc;
    }

    public static SerializeDoc serialize(Object bean, InsertUpdate insertUpdate) throws RepositoryException {
        return serialize(bean, null, insertUpdate);
    }

    private static boolean setPropertyValue(Object object, SerializeField sfield, Map<String, Object> doc,
            ParseBeanOptions options, LinkedList<String> contextKeys) throws Exception {
        Object value = doc.get(sfield.columnName);
        if (value == null) {
            return false;
        }
        Class<?> destClass = sfield.writeMethod.getParameterTypes()[0];
        if (sfield.relationWriteMethod != null) {
            Object relationInstance = destClass.newInstance();
            sfield.writeMethod.invoke(object, relationInstance);
            value = Bean.parse(value, sfield.relationWriteMethod.getParameterTypes()[0]);
            sfield.relationWriteMethod.invoke(relationInstance, value);
        } else {
            ParameterizedType pType = null;
            if (sfield.field != null) {
                Type type = sfield.field.getGenericType();
                if (type instanceof ParameterizedType) {
                    pType = (ParameterizedType) type;
                }
            }
            value = Bean.parse(value, destClass, pType, options, contextKeys);
            sfield.writeMethod.invoke(object, value);
        }
        return true;
    }

    public static <T> T deserialize(Map<String, Object> doc, Class<T> clazz, ParseBeanOptions options)
            throws RepositoryException {
        SerializeMeta meta = getSerializeMeta(clazz);
        try {
            T object = clazz.newInstance();
            LinkedList<String> contextKeys = options != null && options.isInjectContext() ? new LinkedList<String>()
                    : null;
            for (SerializeField sfield : meta.normalFields) {
                setPropertyValue(object, sfield, doc, options, contextKeys);
            }
            if (meta.idField != null) {
                setPropertyValue(object, meta.idField, doc, options, contextKeys);
            }
            return object;
        } catch (RepositoryException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RepositoryException(ex);
        }
    }

    public static <T> T deserialize(Map<String, Object> doc, Class<T> clazz) {
        return deserialize(doc, clazz, null);
    }

    public static String getTable(Class<?> clazz) {
        return clazz.getAnnotation(SerializeBean.class).value();
    }

    public static String getColumnName(Class<?> clazz, String fieldName) {
        SerializeField field = getField(clazz, fieldName);
        return field.columnName;
    }

    public static String getIdColumnName(Class<?> clazz) {
        SerializeMeta meta = getSerializeMeta(clazz);
        return meta.idField.columnName;
    }

    private static SerializeField getField(Class<?> clazz, String fieldName) {
        SerializeMeta meta = getSerializeMeta(clazz);
        if (fieldName.equals(meta.idField.fieldName)) {
            return meta.idField;
        }
        for (SerializeField sfield : meta.normalFields) {
            if (sfield.fieldName.equals(fieldName)) {
                return sfield;
            }
        }
        throw new RepositoryException("Could not find field: " + fieldName);
    }

    public static Object getFieldRawValue(Object entity, String fieldName) {
        SerializeField field = getField(entity.getClass(), fieldName);
        return field.getRawValue(entity);
    }

    public static Object getFieldValue(Object entity, String fieldName) {
        SerializeField field = getField(entity.getClass(), fieldName);
        return field.getValue(entity);
    }

    public static Object getIdValue(Object entity) {
        SerializeMeta meta = getSerializeMeta(entity.getClass());
        return meta.idField.getValue(entity);
    }

}
