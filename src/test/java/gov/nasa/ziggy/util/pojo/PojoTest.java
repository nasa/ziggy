package gov.nasa.ziggy.util.pojo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.hamcrest.beans.PropertyUtil;

import gov.nasa.ziggy.module.io.ProxyIgnore;

/**
 * Tests POJOs.
 *
 * @author Miles Cote
 */
public class PojoTest {
    private PojoTest() {
    }

    public static final void testGettersSetters(Object object) {
        for (Field field : object.getClass().getDeclaredFields()) {
            if (!Modifier.isStatic(field.getModifiers())
                && field.getAnnotation(ProxyIgnore.class) == null) {
                field.setAccessible(true);

                checkIsPrivate(field);
                checkStartsWithLowerCase(field);
                checkPropertyDescriptorExists(field, object);

                Method readMethod = getReadMethod(field, object);
                checkGetterExists(field, readMethod);
                checkGetterStartsWithUpperCase(readMethod);
                checkFieldTypeMatchesGetterType(field, readMethod);
                checkFieldValueMatchesGetterValue(field, object, readMethod);

                if (!Modifier.isFinal(field.getModifiers())) {
                    Method writeMethod = getWriteMethod(field, object);
                    checkSetterExists(field, writeMethod);
                    checkSetterStartsWithUpperCase(writeMethod);
                    checkFieldTypeMatchesSetterType(field, writeMethod);
                    checkFieldValueMatchesSetterValue(field, object, writeMethod);
                }
            }
        }
    }

    private static void checkIsPrivate(Field field) {
        if (!Modifier.isPrivate(field.getModifiers())) {
            throw new FieldAccessException("field must be private." + "\n  field: " + field);
        }
    }

    private static void checkStartsWithLowerCase(Field field) {
        char firstChar = field.getName().charAt(0);
        if (Character.isUpperCase(firstChar)) {
            throw new FieldNameException(
                "field must start with lower case." + "\n  field: " + field);
        }
    }

    private static void checkPropertyDescriptorExists(Field field, Object object) {
        PropertyDescriptor propertyDescriptor = getPropertyDescriptor(field, object);
        if (propertyDescriptor == null) {
            throw new GetterSetterExistenceException("getter must exist." + "\n  field: " + field);
        }
    }

    private static void checkGetterExists(Field field, Method method) {
        if (method == null) {
            throw new GetterSetterExistenceException("getter must exist." + "\n  field: " + field);
        }
    }

    private static void checkSetterExists(Field field, Method method) {
        if (method == null) {
            throw new GetterSetterExistenceException("setter must exist." + "\n  field: " + field);
        }
    }

    private static void checkGetterStartsWithUpperCase(Method method) {
        String methodName = method.getName();

        int upperCaseCharCount = getUpperCaseCharCount(methodName);

        if (upperCaseCharCount != 1) {
            throw new GetterSetterNameException(
                "trimmed getter name must have exactly one upper-case character in its first two characters."
                    + "\n  methodName: " + methodName);
        }
    }

    private static void checkSetterStartsWithUpperCase(Method method) {
        String methodName = method.getName();

        int upperCaseCharCount = getUpperCaseCharCount(methodName);

        if (upperCaseCharCount != 1) {
            throw new GetterSetterNameException(
                "trimmed setter name must have exactly one upper-case character in its first two characters."
                    + "\n  methodName: " + methodName);
        }
    }

    private static int getUpperCaseCharCount(String methodName) {
        String trimmedMethodName = trimPrefix(methodName);

        int upperCaseCharCount = 0;
        if (trimmedMethodName.length() > 0 && Character.isUpperCase(trimmedMethodName.charAt(0))) {
            upperCaseCharCount++;
        }
        if (trimmedMethodName.length() > 1 && Character.isUpperCase(trimmedMethodName.charAt(1))) {
            upperCaseCharCount++;
        }
        return upperCaseCharCount;
    }

    private static void checkFieldTypeMatchesGetterType(Field field, Method readMethod) {
        Class<?> fieldType = field.getType();
        Class<?> getterType = readMethod.getReturnType();
        if (!fieldType.equals(getterType)) {
            throw new GetterSetterTypeException("fieldType must match getterType." + "\n  field: "
                + field + "\n  getterType: " + getterType);
        }
    }

    private static void checkFieldTypeMatchesSetterType(Field field, Method writeMethod) {
        Class<?> fieldType = field.getType();

        Class<?>[] setterTypes = writeMethod.getParameterTypes();
        int setterParameterCount = setterTypes.length;
        if (setterParameterCount != 1) {
            throw new GetterSetterTypeException("setterParameterCount must be exactly 1."
                + "\n  setterParameterCount: " + setterParameterCount);
        }

        Class<?> setterType = setterTypes[0];
        if (!fieldType.equals(setterType)) {
            throw new GetterSetterTypeException("fieldType must match setterType." + "\n  field: "
                + field + "\n  setterType: " + setterType);
        }
    }

    private static void checkFieldValueMatchesGetterValue(Field field, Object object,
        Method readMethod) {
        Object getterValue;
        Object fieldValue;
        try {
            getterValue = readMethod.invoke(object);

            fieldValue = field.get(object);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to get field.", e);
        }

        if (getterValue != fieldValue && !getterValue.equals(fieldValue)) {
            throw new GetterSetterValueException("getterValue must match fieldValue."
                + "\n  getterValue: " + getterValue + "\n  fieldValue: " + fieldValue);
        }
    }

    private static void checkFieldValueMatchesSetterValue(Field field, Object object,
        Method writeMethod) {
        Object setterValue = createTestValue(field);

        Object fieldValue;
        try {
            writeMethod.invoke(object, setterValue);

            fieldValue = field.get(object);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to get field.", e);
        }

        if (setterValue != fieldValue && !setterValue.equals(fieldValue)) {
            throw new GetterSetterValueException("setterValue must match fieldValue."
                + "\n  setterValue: " + setterValue + "\n  fieldValue: " + fieldValue);
        }
    }

    private static Method getWriteMethod(Field field, Object object) {
        PropertyDescriptor propertyDescriptor = getPropertyDescriptor(field, object);

        Method writeMethod = propertyDescriptor.getWriteMethod();
        return writeMethod;
    }

    private static String trimPrefix(String methodName) {
        String trimmedMethodName;
        if (methodName.startsWith("is")) {
            trimmedMethodName = methodName.substring(2, methodName.length());
        } else if (methodName.startsWith("get")) {
            trimmedMethodName = methodName.substring(3, methodName.length());
        } else if (methodName.startsWith("set")) {
            trimmedMethodName = methodName.substring(3, methodName.length());
        } else {
            throw new IllegalArgumentException(
                "Unexpected methodName." + "\n  methodName: " + methodName);
        }

        return trimmedMethodName;
    }

    private static PropertyDescriptor getPropertyDescriptor(Field field, Object object) {
        PropertyDescriptor propertyDescriptor;
        try {
            propertyDescriptor = PropertyUtil.getPropertyDescriptor(field.getName(), object);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unable to get property descriptor.", e);
        }

        return propertyDescriptor;
    }

    private static Method getReadMethod(Field field, Object object) {
        PropertyDescriptor propertyDescriptor = getPropertyDescriptor(field, object);

        Method readMethod = propertyDescriptor.getReadMethod();
        return readMethod;
    }

    private static Object createTestValue(Field field) {
        Class<?> fieldType = field.getType();
        String fieldName = field.getName();
        int fieldHashCode = fieldName.hashCode();

        Object testValue;
        if (fieldType.equals(boolean.class)) {
            testValue = true;
        } else if (fieldType.equals(char.class)) {
            testValue = (char) fieldHashCode;
        } else if (fieldType.equals(byte.class)) {
            testValue = (byte) fieldHashCode;
        } else if (fieldType.equals(short.class)) {
            testValue = (short) fieldHashCode;
        } else if (fieldType.equals(int.class)) {
            testValue = (int) fieldHashCode;
        } else if (fieldType.equals(long.class)) {
            testValue = (long) fieldHashCode;
        } else if (fieldType.equals(float.class)) {
            testValue = (float) fieldHashCode;
        } else if (fieldType.equals(double.class)) {
            testValue = (double) fieldHashCode;
        } else if (fieldType.equals(String.class)) {
            testValue = fieldName;
        } else {
            testValue = null;
        }

        return testValue;
    }

    @SafeVarargs
    public static final <T> void testToStringHashCodeEquals(T instance, T instanceWithSameKeys,
        T... instancesWithDifferentKey) {
        testToString(instance, instanceWithSameKeys, instancesWithDifferentKey);
        testHashCodeEquals(instance, instanceWithSameKeys, instancesWithDifferentKey);
    }

    @SafeVarargs
    public static final <T> void testToString(T instance, T instanceWithSameKeys,
        T... instancesWithDifferentKey) {
        assertTrue(instance.toString().equals(instanceWithSameKeys.toString()));

        for (T instanceWithDifferentKey : instancesWithDifferentKey) {
            assertTrue(!instance.toString().equals(instanceWithDifferentKey.toString()));
        }
    }

    @SafeVarargs
    public static final <T> void testHashCodeEquals(T instance, T instanceWithSameKeys,
        T... instancesWithDifferentKey) {
        assertTrue(instance.hashCode() == instanceWithSameKeys.hashCode());
        assertTrue(instance.equals(instanceWithSameKeys));

        for (T instanceWithDifferentKey : instancesWithDifferentKey) {
            assertTrue(instance.hashCode() != instanceWithDifferentKey.hashCode());
            assertTrue(!instance.equals(instanceWithDifferentKey));
            assertTrue(!instanceWithDifferentKey.equals(instance));
        }

        assertTrue(instance.equals(instance));
        assertNotNull(instance);
        assertTrue(!instance.equals(null));
        assertTrue(!instance.equals(new Object()));
    }

    public static final <T extends Comparable<T>> void testCompareTo(T instance,
        T instanceWithSameKeys, T instanceWithSmallerKeys, T instanceWithLargerKeys) {
        assertTrue(instance.compareTo(instanceWithLargerKeys) < 0);
        assertTrue(instance.compareTo(instanceWithSameKeys) == 0);
        assertTrue(instance.compareTo(instanceWithSmallerKeys) > 0);

        assertTrue(instance.compareTo(instance) == 0);

        List<T> list = new ArrayList<>();
        list.add(instanceWithLargerKeys);
        list.add(instance);
        list.add(instanceWithSmallerKeys);
        list.add(instanceWithSameKeys);

        Collections.sort(list);

        List<T> expectedList = new ArrayList<>();
        expectedList.add(instanceWithSmallerKeys);
        expectedList.add(instance);
        expectedList.add(instanceWithSameKeys);
        expectedList.add(instanceWithLargerKeys);

        assertEquals(expectedList, list);
    }
}
