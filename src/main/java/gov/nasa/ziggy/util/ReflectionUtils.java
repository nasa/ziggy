package gov.nasa.ziggy.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import gov.nasa.ziggy.module.io.ProxyIgnore;

/**
 * Utility methods for reflection.
 *
 * @author PT
 */
public class ReflectionUtils {

    public static List<Field> getAllFields(Class<?> clazz, boolean includeProxyIgnoreFields) {
        List<Field> fields = new ArrayList<>();
        Field[] declaredFields = clazz.getDeclaredFields();
        for (Field field : declaredFields) {
            boolean includeField = !Modifier.isStatic(field.getModifiers());
            includeField = includeField
                && (includeProxyIgnoreFields || field.getAnnotation(ProxyIgnore.class) == null);
            if (includeField) {
                fields.add(field);
            }
        }
        Class<?> parentClass = clazz.getSuperclass();
        if (parentClass != null) {
            fields.addAll(getAllFields(parentClass, includeProxyIgnoreFields));
        }
        return fields;
    }

    public static List<Field> getAllFields(Object obj, boolean includeProxyIgnoreFields) {
        return getAllFields(obj.getClass(), includeProxyIgnoreFields);
    }
}
