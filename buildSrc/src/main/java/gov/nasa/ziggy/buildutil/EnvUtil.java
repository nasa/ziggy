package gov.nasa.ziggy.buildutil;

import java.util.NoSuchElementException;

/**
 * Environment variables utilities.
 * 
 * @author Sean McCauliff
 *
 */
public class EnvUtil {

    /**
     * @param key non-null environment variable key.
     * @return the environment variable
     * @exception NoSuchElementException if the key does not exist.
     */
    public static String environment(String key) {
        String value = System.getenv(key);
        if (value == null) {
            throw new NoSuchElementException("Environment variable \"" + key + "\" is not set.");
        }
        return value;
    }

    public static String environment(String key, String defaultValue) {
        String value = System.getenv(key);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }
}
