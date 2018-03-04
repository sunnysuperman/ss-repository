package com.sunnysuperman.repository.serialize;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.reflections.Reflections;

import com.sunnysuperman.commons.bean.Bean;
import com.sunnysuperman.commons.model.ObjectId;
import com.sunnysuperman.commons.util.FormatUtil;
import com.sunnysuperman.commons.util.StringUtil;
import com.sunnysuperman.repository.InsertUpdate;

public class SerializeBeanUtils {

    private static class SerializeMeta {
        private List<SerializeField> normalFields;
        private SerializeField idField;
        private SerializeId idInfo;
    }

    private static class SerializeField {
        private String columnName;
        private Method readMethod;
        private Method writeMethod;
        private Method relationWriteMethod;
        private Method relationReadMethod;
        private SerializeProperty property;

        public Object getValue(Object bean) throws Exception {
            Object value = readMethod.invoke(bean);
            if (value == null) {
                return null;
            }
            if (property.relation() == Relation.NONE) {
                return value;
            }
            return relationReadMethod.invoke(value);
        }
    }

    private static Map<Class<?>, SerializeMeta> META_MAP = new HashMap<>();

    private static SerializeMeta getSerializeMeta(Class<?> clazz) throws Exception {
        SerializeMeta meta = new SerializeMeta();
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
            if (!property.insertable() && !property.updatable()) {
                throw new RuntimeException(clazz + "." + fieldName + " is neither insertable nor updatable");
            }
            String columnName = property.column();
            if (columnName == null || columnName.isEmpty()) {
                columnName = fieldName;
            }
            columnName = StringUtil.camel2underline(columnName);
            if (columnNames.contains(columnName)) {
                throw new RuntimeException("Duplicated column '" + columnName + "' in " + clazz);
            }
            columnNames.add(columnName);
            SerializeField sfield = new SerializeField();
            sfield.readMethod = method;
            sfield.writeMethod = clazz.getMethod("set" + capitalize(fieldName), method.getReturnType());
            sfield.columnName = columnName;
            sfield.property = property;
            if (property.relation() != Relation.NONE) {
                sfield.relationWriteMethod = getRelationWriteMethod(method, property, columnName);
                sfield.relationReadMethod = getRelationReadMethod(method, property, columnName);
            }
            if (field != null && field.getAnnotation(SerializeId.class) != null) {
                if (idField != null) {
                    throw new RuntimeException("Duplicated id column of " + clazz);
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
            throw new RuntimeException(
                    readMethod.getClass() + "." + columnName + ": could not get relation write method");
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
            throw new RuntimeException(
                    readMethod.getClass() + "." + columnName + ": could not get relation read method");
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
        Reflections reflections = new Reflections(packageName);
        Set<Class<?>> classes = reflections.getTypesAnnotatedWith(SerializeBean.class);
        for (Class<?> clazz : classes) {
            META_MAP.put(clazz, getSerializeMeta(clazz));
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
                throw new RuntimeException("Failed to convert " + id.getClass() + " to " + idClass);
            }
        }
        try {
            meta.idField.writeMethod.invoke(bean, convertedId);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    public static SerializeDoc serialize(Object bean, InsertUpdate insertUpdate) throws Exception {
        SerializeMeta meta = META_MAP.get(bean.getClass());
        if (meta == null) {
            throw new RuntimeException(bean.getClass() + " is not annotated with SerializeBean");
        }
        SerializeDoc sdoc = new SerializeDoc();
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
            case RUNTIME:
                update = id != null;
                break;
            default:
                throw new RuntimeException("Unknown InsertUpdate");
            }
        }
        if (update) {
            if (id == null) {
                throw new RuntimeException("No id set to update");
            }
            sdoc.setIdValues(new Object[] { id });
            if (insertUpdate == InsertUpdate.RUNTIME) {
                sdoc.setUpsert(meta.idInfo.strategy() == IdGenerator.PROVIDE);
            }
        } else {
            SerializeId idInfo = meta.idInfo;
            if (idInfo != null) {
                switch (idInfo.strategy()) {
                case INCREMENT:
                    sdoc.setIdIncrementClass(idInfo.incrementClass());
                    break;
                case UUID:
                    id = UUID.randomUUID().toString().replaceAll("-", "");
                    break;
                case OBJECTID:
                    id = new ObjectId().toHexString();
                    break;
                case PROVIDE:
                    break;
                default:
                    throw new RuntimeException("Unsupported id generator " + idInfo.strategy());
                }
            }
        }
        Map<String, Object> doc = new HashMap<>();
        sdoc.setDoc(doc);
        for (SerializeField sfield : meta.normalFields) {
            SerializeProperty property = sfield.property;
            if (update) {
                if (!property.updatable()) {
                    continue;
                }
            } else if (!property.insertable()) {
                continue;
            }
            doc.put(sfield.columnName, sfield.getValue(bean));
        }
        if (!update && id != null) {
            doc.put(sdoc.getIdColumns()[0], id);
        }
        return sdoc;
    }

    private static boolean setPropertyValue(Object object, SerializeField sfield, Map<String, Object> doc)
            throws Exception {
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
            value = Bean.parse(value, destClass);
            sfield.writeMethod.invoke(object, value);
        }
        return true;
    }

    public static <T> T deserialize(Map<String, Object> doc, Class<T> clazz) throws Exception {
        SerializeMeta meta = META_MAP.get(clazz);
        if (meta == null) {
            throw new RuntimeException(clazz + " is not annotated with SerializeBean");
        }
        T object = clazz.newInstance();
        for (SerializeField sfield : meta.normalFields) {
            setPropertyValue(object, sfield, doc);
        }
        setPropertyValue(object, meta.idField, doc);
        return object;
    }
}
