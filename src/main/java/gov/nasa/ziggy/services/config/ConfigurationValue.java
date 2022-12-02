package gov.nasa.ziggy.services.config;

/**
 * Implements a wrapper around a configuration value.
 */
public class ConfigurationValue {

    private String propertyName;
    private String defaultValue;
    private Class<?> type;

    public ConfigurationValue(String propertyName, String defaultValue, Class<?> type) {
        this.propertyName = propertyName;
        this.defaultValue = defaultValue;
        this.type = type;

        checkDefaultValue(defaultValue, type);
    }

    private void checkDefaultValue(String defaultValue, Class<?> type) {
        if (type == Integer.TYPE) {
            Integer.parseInt(defaultValue);
        } else if (type == Long.TYPE) {
            Long.parseLong(defaultValue);
        } else if (type == Boolean.TYPE) {
            Boolean.parseBoolean(defaultValue);
        } // Else no checking is performed.
    }

    private void checkAccessType(Class<?> attemptedType) {
        if (attemptedType != type) {
            throw new WrongParameterTypeException(propertyName, type);
        }
    }

    /**
     * Gets the configuration parameter value, as a string.
     *
     * @return the configuration parameter value
     */
    public String getString() {
        checkAccessType(String.class);
        return ZiggyConfiguration.getInstance().getString(propertyName, defaultValue);
    }

    /**
     * Gets the configuration parameter value, as an int.
     *
     * @return the configuration parameter value
     */
    public int getInt() {
        checkAccessType(Integer.TYPE);
        return ZiggyConfiguration.getInstance()
            .getInt(propertyName, Integer.parseInt(defaultValue));
    }

    /**
     * Gets the configuration parameter value, as a long.
     *
     * @return the configuration parameter value
     */
    public long getLong() {
        checkAccessType(Long.TYPE);
        return ZiggyConfiguration.getInstance().getLong(propertyName, Long.parseLong(defaultValue));
    }

    /**
     * Gets the configuration parameter value, as a float.
     *
     * @return the configuration parameter value
     */
    public float getFloat() {
        checkAccessType(Float.TYPE);
        return ZiggyConfiguration.getInstance()
            .getFloat(propertyName, Float.parseFloat(defaultValue));
    }

    /**
     * Gets the configuration parameter value, as a double.
     *
     * @return the configuration parameter value
     */
    public double getDouble() {
        checkAccessType(Double.TYPE);
        return ZiggyConfiguration.getInstance()
            .getDouble(propertyName, Double.parseDouble(defaultValue));
    }

    /**
     * Gets the configuration parameter value, as a boolean.
     *
     * @return the configuration parameter value
     */
    public boolean getBoolean() {
        checkAccessType(Boolean.TYPE);
        return ZiggyConfiguration.getInstance()
            .getBoolean(propertyName, Boolean.parseBoolean(defaultValue));
    }

    /**
     * Gets the class of the configuration value.
     *
     * @return the class of the value
     */
    public Class<?> getValueClass() {
        return type;
    }

    /**
     * Represents an exception thrown when a parameter is accessed using the wrong type accessor
     * method.
     */
    @SuppressWarnings("serial")
    public static class WrongParameterTypeException extends RuntimeException {

        public WrongParameterTypeException(String parameterName, Class<?> type) {
            super("Parameter '" + parameterName + "' must be accessed as " + type.getSimpleName());
        }
    }

}
