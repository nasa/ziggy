package gov.nasa.ziggy.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import gov.nasa.ziggy.module.io.ProxyIgnore;

public class ReflectionUtilsTest {

    @Test
    public void testGetAllFieldsOfClass() {
        checkFields(ReflectionUtils.getAllFields(ReflectionSample.class, false), false);
        checkFields(ReflectionUtils.getAllFields(ReflectionSample.class, true), true);
    }

    @Test
    public void testGetAllFieldsOfObject() {
        checkFields(ReflectionUtils.getAllFields(new ReflectionSample(), false), false);
        checkFields(ReflectionUtils.getAllFields(new ReflectionSample(), true), true);
    }

    private void checkFields(List<Field> fields, boolean includeProxyIgnoreFields) {
        assertEquals(includeProxyIgnoreFields ? 5 : 3, fields.size());

        List<String> fieldNames = fields.stream().map(Field::getName).collect(Collectors.toList());

        assertTrue(fieldNames.contains("stringValue"));
        assertTrue(fieldNames.contains("intValue"));
        assertTrue(fieldNames.contains("floatValue"));

        if (includeProxyIgnoreFields) {
            assertTrue(fieldNames.contains("ignoredString"));
            assertTrue(fieldNames.contains("ignoredInt"));
        }
    }

    private static class ReflectionSample {
        @SuppressWarnings("unused")
        private static final int SOME_STATIC_CONSTANT = 42;

        @SuppressWarnings("unused")
        private String stringValue;
        @SuppressWarnings("unused")
        private Integer intValue;
        @SuppressWarnings("unused")
        private Float floatValue;

        @ProxyIgnore
        private String ignoredString;
        @ProxyIgnore
        private int ignoredInt;
    }
}
