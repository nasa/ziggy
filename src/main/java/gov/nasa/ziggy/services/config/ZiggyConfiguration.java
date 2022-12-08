package gov.nasa.ziggy.services.config;

import java.io.File;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.NoSuchElementException;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;

/**
 * @author Todd Klaus
 */
public class ZiggyConfiguration {
    private static final Logger log = LoggerFactory.getLogger(ZiggyConfiguration.class);

    /**
     * Default directory used to locate property file if the PIPELINE_CONFIG_PATH environment
     * variable is not defined.
     */
    public static final String CONFIG_SERVICE_PROPERTIES_DEFAULT_DIR = "etc";

    /**
     * Default property file used if the PIPELINE_CONFIG_PATH environment variable is not defined.
     */
    public static final String CONFIG_SERVICE_PROPERTIES_DEFAULT_FILE = "ziggy.properties";

    private static final String ZIGGY_RELATIVE_PATH = "ziggy";

    private static Configuration instance = null;
    private static AbstractConfiguration unsynchronizedInstance = null;

    public static final String CONFIG_SERVICE_PROPERTIES_PATH_ENV = "PIPELINE_CONFIG_PATH";

    public ZiggyConfiguration() {
    }

    /**
     * Construct a composite configuration. The properties are stored in the following hierarchy:
     * <ol>
     * <li>System properties have the highest priority.
     * <li>After system properties, properties in the pipeline config file, defined by the
     * environment variable PIPELINE_CONFIG_PATH, are used.
     * <li>After system properties and pipeline properties, properties in the Ziggy config file are
     * used. This file is specified by the pipeline config file using the ziggy.config.path
     * property. If no property is specified in the config file, Ziggy will default to the
     * ziggy.properties file in the config directory.
     * </ol>
     *
     * @return A composite configuration with priority of values as described above.
     */
    private static AbstractConfiguration getConfiguration() {
        log.debug("initialize() - start");
        CompositeConfiguration config = new ConfigurationDelegate();

        // copy the system properties into the composite configuration
        config.addConfiguration(new SystemConfiguration());

        // Load the pipeline configurations from the file pointed at by PIPELINE_CONFIG_PATH;
        // if that cannot be found, attempt to load a default config file
        File configPropertiesFile = getConfigServicesFile();

        if (!configPropertiesFile.exists()) {
            throw new PipelineException("Config file pointed to by the "
                + CONFIG_SERVICE_PROPERTIES_PATH_ENV + " environment variable does not exist: "
                + configPropertiesFile.getAbsolutePath());
        }
        log.info("Loading configuration from: " + configPropertiesFile.getAbsolutePath());
        try {
            config.addConfiguration(new PropertiesConfiguration(configPropertiesFile));
            String ziggyConfigFilename = config.getString(PropertyNames.ZIGGY_CONFIG_PROP_NAME,
                null);
            if (ziggyConfigFilename != null && !ziggyConfigFilename.isEmpty()) {
                File ziggyConfigFile = new File(ziggyConfigFilename);
                if (ziggyConfigFile.exists()) {
                    log.info("Loading configuration from: " + ziggyConfigFile.getAbsolutePath());
                    config.addConfiguration(new PropertiesConfiguration(ziggyConfigFile));
                }
            }
        } catch (Exception e) {
            log.error("ConfigurationService failed to initialize", e);
            throw new PipelineException("ConfigurationService failed to initialize", e);
        }

        log.debug("initialize() - end");
        return config;

    }

    public static synchronized AbstractConfiguration unsynchronizedInstance() {
        getInstance();
        return unsynchronizedInstance;
    }

    public static synchronized Configuration getInstance() {
        if (instance == null) {
            Configuration config = getConfiguration();
            unsynchronizedInstance = (AbstractConfiguration) config;
            // TODO Update to configuration2
            // Then, call config.setSynchronizer(ZiggyConfiguration.class) in getConfiguration() and
            // delete the following.
            InvocationHandler handler = new SynchronizedInvocationHandler(config);
            instance = (Configuration) Proxy.newProxyInstance(Configuration.class.getClassLoader(),
                new Class<?>[] { Configuration.class }, handler);
        }
        return instance;
    }

    public static synchronized void reset() {
        instance = null;
        unsynchronizedInstance = null;
    }

    /**
     * Gets the value of an environment variable, unless we are running in a unit test. When run
     * inside a unit test, always return null for environment variables. To determine whether a unit
     * test is running, we use a system property that is set by the Gradle <code>test</code> task.
     *
     * @param envVarName the environment variable name
     * @return the environment variable value, or null if the environment variable is not defined or
     * if we are running inside a unit test
     */
    private static String getEnvVar(String envVarName) {
        if (System.getProperty("org.gradle.test.worker") != null) {
            return null;
        } else {
            return System.getenv(envVarName);
        }
    }

    /**
     * Locates the pipeline config file via the PIPELINE_CONFIG_PATH environment variable. If no
     * environment variable is defined, or the file it points to does not exist, the default
     * properties file (ziggy/etc/ziggy.properties) will be used if it can be found.
     */
    public static File getConfigServicesFile() {
        File configServicesFile = null;
        String configFileEnvValue = getEnvVar(CONFIG_SERVICE_PROPERTIES_PATH_ENV);
        log.debug("found environment variable: " + CONFIG_SERVICE_PROPERTIES_PATH_ENV + " = "
            + configFileEnvValue);
        if (configFileEnvValue != null) {
            configServicesFile = new File(configFileEnvValue);
        }
        if (configServicesFile == null || !configServicesFile.exists()) {
            Path ziggyDefaultConfig = Paths.get(
                ziggyRoot(System.getProperty(PropertyNames.CURRENT_DIR_PROP_NAME)),
                CONFIG_SERVICE_PROPERTIES_DEFAULT_DIR, CONFIG_SERVICE_PROPERTIES_DEFAULT_FILE);
            configServicesFile = ziggyDefaultConfig.toFile();
        }
        if (!configServicesFile.exists()) {
            throw new PipelineException("Unable to locate config file");
        }

        return configServicesFile;
    }

    public static String ziggyRoot(String directory) {
        if (directory == null || directory.isEmpty()) {
            return null;
        }

        // Does this path appear to contain "ziggy"?
        int ziggyLocation = directory.lastIndexOf(ZIGGY_RELATIVE_PATH);
        if (ziggyLocation < 0) {
            return null;
        }

        // Does the path have directories after "ziggy"?
        int separatorLocation = directory.indexOf(File.separator, ziggyLocation);

        // If so, remove them.
        return separatorLocation > 0 ? directory.substring(0, separatorLocation) : directory;
    }

    /**
     * Wraps Configuration in order to throw {@code NoSuchElementException} if a property is missing
     * for calls that don't supply a default.
     * <p>
     * This is necessary because the setThrowExceptionOnMissing() method doesn't work on primitives.
     */
    private static class ConfigurationDelegate extends CompositeConfiguration {

        /** Throws {@code NoSuchElementException} if {@code key} is missing. */
        private void checkProperty(String key) {
            if (!containsKey(key)) {
                throw new NoSuchElementException(key + ": No such property");
            }
        }

        @Override
        public BigDecimal getBigDecimal(String key) {
            checkProperty(key);
            return super.getBigDecimal(key);
        }

        @Override
        public BigInteger getBigInteger(String key) {
            checkProperty(key);
            return super.getBigInteger(key);
        }

        @Override
        public boolean getBoolean(String key) {
            checkProperty(key);
            return super.getBoolean(key);
        }

        @Override
        public byte getByte(String key) {
            checkProperty(key);
            return super.getByte(key);
        }

        @Override
        public double getDouble(String key) {
            checkProperty(key);
            return super.getDouble(key);
        }

        @Override
        public float getFloat(String key) {
            checkProperty(key);
            return super.getFloat(key);
        }

        @Override
        public int getInt(String key) {
            checkProperty(key);
            return super.getInt(key);
        }

        @Override
        public long getLong(String key) {
            checkProperty(key);
            return super.getLong(key);
        }

        @Override
        public short getShort(String key) {
            checkProperty(key);
            return super.getShort(key);
        }

        @Override
        public String getString(String key) {
            checkProperty(key);
            return super.getString(key);
        }
    }

    /**
     * Implements a proxy invocation handler that synchronizes all calls on the base object.
     */
    private static class SynchronizedInvocationHandler implements InvocationHandler {

        private Object baseObject;

        /**
         * Creates a new instance that will route all invocations to the given object.
         *
         * @param baseObject the object that will get all invocations
         */
        public SynchronizedInvocationHandler(Object baseObject) {
            this.baseObject = baseObject;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            synchronized (baseObject) {
                return method.invoke(baseObject, args);
            }
        }
    }

}
