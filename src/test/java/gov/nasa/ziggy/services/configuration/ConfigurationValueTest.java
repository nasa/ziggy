package gov.nasa.ziggy.services.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

import gov.nasa.ziggy.services.config.ConfigurationValue;

/**
 * Implements unit tests for {@link ConfigurationValue}.
 */
public class ConfigurationValueTest {

    private static final String NON_EXISTENT_PROPERTY = "does-not-exist";

    private static final Object[][] VALUE_ATTRS = { { "value", String.class },
        { "123", Integer.TYPE }, { "123", Long.TYPE }, { "3.14", Float.TYPE },
        { "3.14", Double.TYPE }, { "true", Boolean.TYPE }, };

    /**
     * Tests that access to the value can be performed using the correct accessor methods.
     */
    @Test
    public void testValidAndInvalidAccess() {
        for (Object[] valueAttr : VALUE_ATTRS) {
            String valueStr = (String) valueAttr[0];
            Class<?> type = (Class<?>) valueAttr[1];
            ConfigurationValue value = new ConfigurationValue(NON_EXISTENT_PROPERTY, valueStr,
                type);

            if (type == String.class) {
                assertEquals(valueStr, value.getString());
            } else if (type == Integer.TYPE) {
                assertEquals(Integer.parseInt(valueStr), value.getInt());
            } else if (type == Long.TYPE) {
                assertEquals(Long.parseLong(valueStr), value.getLong());
            } else if (type == Float.TYPE) {
                assertEquals(Float.parseFloat(valueStr), value.getFloat(), Float.MIN_NORMAL);
            } else if (type == Double.TYPE) {
                assertEquals(Double.parseDouble(valueStr), value.getDouble(), Double.MIN_NORMAL);
            } else if (type == Boolean.TYPE) {
                assertEquals(Boolean.parseBoolean(valueStr), value.getBoolean());
            }
        }
    }

    /**
     * Tests that calls to the wrong accessor method for a value will throw an exception.
     */
    @Test
    public void testInvalidAccess() {
        for (Object[] valueAttr : VALUE_ATTRS) {
            String valueStr = (String) valueAttr[0];
            Class<?> type = (Class<?>) valueAttr[1];
            checkIllegalAccess(new ConfigurationValue(NON_EXISTENT_PROPERTY, valueStr, type));
        }
    }

    private void checkIllegalAccess(ConfigurationValue value) {
        Class<?> type = value.getValueClass();

        if (type != String.class) {
            try {
                value.getString();
                fail("Access as a string should have failed");
            } catch (ConfigurationValue.WrongParameterTypeException ex) {
                // ignore
            }
        }
        if (type != Integer.TYPE) {
            try {
                value.getInt();
                fail("Access as an int should have failed");
            } catch (ConfigurationValue.WrongParameterTypeException ex) {
                // ignore
            }
        }
        if (type != Long.TYPE) {
            try {
                value.getLong();
                fail("Access as a long should have failed");
            } catch (ConfigurationValue.WrongParameterTypeException ex) {
                // ignore
            }
        }
        if (type != Float.TYPE) {
            try {
                value.getFloat();
                fail("Access as a float should have failed");
            } catch (ConfigurationValue.WrongParameterTypeException ex) {
                // ignore
            }
        }
        if (type != Double.TYPE) {
            try {
                value.getDouble();
                fail("Access as a double should have failed");
            } catch (ConfigurationValue.WrongParameterTypeException ex) {
                // ignore
            }
        }
        if (type != Boolean.TYPE) {
            try {
                value.getBoolean();
                fail("Access as a boolean should have failed");
            } catch (ConfigurationValue.WrongParameterTypeException ex) {
                // ignore
            }
        }
    }

}
