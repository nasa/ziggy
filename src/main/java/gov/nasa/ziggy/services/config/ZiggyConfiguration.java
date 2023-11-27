package gov.nasa.ziggy.services.config;

import static gov.nasa.ziggy.services.config.PropertyName.JAVA_RUNTIME_NAME;
import static gov.nasa.ziggy.services.config.PropertyName.JAVA_VM_VERSION;
import static gov.nasa.ziggy.services.config.PropertyName.SUN_BOOT_LIBRARY_PATH;
import static gov.nasa.ziggy.services.config.PropertyName.ZIGGY_CONFIG_PATH;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;

import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.commons.configuration2.SystemConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.interpol.ConfigurationInterpolator;
import org.apache.commons.configuration2.sync.ReadWriteSynchronizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * The Ziggy configuration. Most code will just use {@link #getInstance()}. This class is
 * thread-safe.
 *
 * @author Todd Klaus
 * @author PT
 * @author Bill Wohler
 */
public class ZiggyConfiguration {
    private static final String BUILD_CONFIGURATION = "ziggy-build.properties";

    private static final Logger log = LoggerFactory.getLogger(ZiggyConfiguration.class);

    public static final String ZIGGY_HOME_ENV = "ZIGGY_HOME";
    public static final String PIPELINE_CONFIG_PATH_ENV = "PIPELINE_CONFIG_PATH";

    /**
     * Default directory used to locate property file if the PIPELINE_CONFIG_PATH environment
     * variable is not defined.
     */
    public static final String PIPELINE_CONFIG_DEFAULT_DIR = "etc";

    /**
     * Default property file used if the PIPELINE_CONFIG_PATH environment variable is not defined.
     */
    public static final String PIPELINE_CONFIG_DEFAULT_FILE = "ziggy.properties";

    private static ImmutableConfiguration instance;
    private static Configuration mutableInstance;
    private static ConfigurationInterpolator interpolator;

    /**
     * Returns an immutable configuration object with the content of the following in order of
     * priority:
     * <ol>
     * <li>System properties
     * <li>Properties in the pipeline configuration file, defined by the environment variable
     * {@value #PIPELINE_CONFIG_PATH_ENV}, except when run in a test.
     * <li>Properties in the Ziggy configuration file. This file is specified by the pipeline
     * configuration file using the {@link PropertyName#ZIGGY_CONFIG_PATH} property. If that
     * property is missing, then Ziggy will use the {@value #PIPELINE_CONFIG_DEFAULT_FILE} file in
     * the {@value #PIPELINE_CONFIG_DEFAULT_DIR} directory.
     * </ol>
     * This object's throwExceptionOnMissing property is set to {@code true}. In the rare case that
     * a {@code NoSuchElementException} is not desired if the property is missing, provide a default
     * when obtaining a property.
     * <p>
     * Classes under test will instead use an immutable version of the configuration obtained with
     * {@link #getMutableInstance()} by the test.
     */
    public static synchronized ImmutableConfiguration getInstance() {
        if (instance == null) {
            Configuration configuration = getConfiguration();
            instance = ConfigurationUtils.unmodifiableConfiguration(configuration);
            interpolator = configuration.getInterpolator();
        }
        return instance;
    }

    /**
     * Returns a configuration as described in {@link #getInstance()}.
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private static Configuration getConfiguration() {
        CompositeConfiguration config = new CompositeConfiguration();
        config.setThrowExceptionOnMissing(true);

        loadSystemConfiguration(config);

        // We know that we are in a test environment if there's a mutable instance.
        // However, in rare cases, the mutable instance is null even though we are
        // in a test environment. In those cases, the test can define the
        // TEST_ENVIRONMENT as a system property to accomplish the same thing.
        boolean testConfiguration = mutableInstance != null
            || config.getString(PropertyName.TEST_ENVIRONMENT.toString(), null) != null;

        if (!testConfiguration) {
            loadPipelineConfiguration(config);
        } else if (mutableInstance != null) {
            config.addConfiguration(mutableInstance);
        }
        loadZiggyConfiguration(config);
        loadBuildConfiguration(config);

        return config;
    }

    /**
     * Copies the system properties into the composite configuration.
     *
     * @param config the non-{@code null} target composite configuration
     */
    private static void loadSystemConfiguration(CompositeConfiguration config) {
        config.addConfiguration(new SystemConfiguration());
    }

    /**
     * Loads the pipeline configuration from the file pointed at by PIPELINE_CONFIG_PATH.
     *
     * @param config the non-{@code null} target composite configuration
     */
    private static void loadPipelineConfiguration(CompositeConfiguration config) {

        File configPropertiesFile = getPipelineConfigFile();
        if (configPropertiesFile != null) {
            loadConfiguration(config, configPropertiesFile);
        }
    }

    /**
     * Loads the Ziggy configuration in {@link PropertyName#ZIGGY_CONFIG_PATH},
     * {@link PropertyName#ZIGGY_HOME_DIR}{@code /etc/ziggy.properties}, or
     * {@link ZIGGY_HOME}{@code /etc/ziggy.properties} in that order.
     *
     * @param config the non-{@code null} target composite configuration
     */
    private static void loadZiggyConfiguration(CompositeConfiguration config) {
        String ziggyConfigFilename = config.getString(ZIGGY_CONFIG_PATH.property(), null);

        Path ziggyConfigPath = null;
        if (ziggyConfigFilename != null && !ziggyConfigFilename.isEmpty()) {
            ziggyConfigPath = Paths.get(ziggyConfigFilename);
        }
        if (ziggyConfigPath == null) {
            // Alas, the code for DirectoryProperties.ziggyHomeDir() is duplicated here to avoid an
            // infinite loop.
            String ziggyHomePath = config.getString(PropertyName.ZIGGY_HOME_DIR.property(),
                System.getenv(ZIGGY_HOME_ENV));
            Path ziggyHomeDir = ziggyHomePath != null ? Paths.get(ziggyHomePath) : null;
            if (ziggyHomeDir != null && !ziggyHomeDir.toString().isEmpty()) {
                ziggyConfigPath = ziggyHomeDir.resolve(PIPELINE_CONFIG_DEFAULT_DIR)
                    .resolve(PIPELINE_CONFIG_DEFAULT_FILE);
                log.warn("{} not defined in {}, trying {}", ZIGGY_CONFIG_PATH,
                    PIPELINE_CONFIG_PATH_ENV, ziggyConfigPath);
            }
        }

        if (ziggyConfigPath == null) {
            log.warn("Could not locate Ziggy configuration");
        } else if (!Files.exists(ziggyConfigPath)) {
            log.warn("Ziggy configuration in {} not found", ziggyConfigPath);
        } else {
            loadConfiguration(config, ziggyConfigPath.toFile());
        }
    }

    /**
     * Loads the build configuration from the {@value #BUILD_CONFIGURATION} file(s).
     *
     * @param config the non-{@code null} target composite configuration
     */
    private static void loadBuildConfiguration(CompositeConfiguration config) {
        try {
            Enumeration<URL> buildUrl = ZiggyConfiguration.class.getClassLoader()
                .getResources(BUILD_CONFIGURATION);
            if (buildUrl.hasMoreElements()) {
                while (buildUrl.hasMoreElements()) {
                    loadConfiguration(config, new File(buildUrl.nextElement().getPath()));
                }
            } else {
                log.warn("Could not locate build information in {}", BUILD_CONFIGURATION);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                "Unable to load configuration from " + BUILD_CONFIGURATION, e);
        }
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private static void loadConfiguration(CompositeConfiguration config, File propertiesFile) {
        try {
            log.info("Loading configuration from: {}", propertiesFile.getAbsolutePath());
            config.addConfiguration(new Configurations().properties(propertiesFile));
        } catch (ConfigurationException e) {
            throw new PipelineException("Ziggy configuration failed to initialize", e);
        }
    }

    /**
     * Locates the pipeline configuration file. This file is defined by the environment variable
     * {@value #PIPELINE_CONFIG_PATH_ENV}.
     *
     * @return null if the environment variable isn't present; otherwise, a file object for the file
     * represented by the variable
     * @throws PipelineException if the file referenced by the variable doesn't exist
     */
    private static File getPipelineConfigFile() {
        String configFileEnvValue = System.getenv(PIPELINE_CONFIG_PATH_ENV);
        log.debug("{}={}", PIPELINE_CONFIG_PATH_ENV, configFileEnvValue);

        File configFile = configFileEnvValue != null ? configFile = new File(configFileEnvValue)
            : null;
        if (configFile != null && !configFile.exists()) {
            throw new PipelineException("Config file pointed to by the " + PIPELINE_CONFIG_PATH_ENV
                + " environment variable does not exist: " + configFile.getAbsolutePath());
        }

        return configFile;
    }

    /** Interpolates the given object with this configuration. */
    public static Object interpolate(Object o) {
        // Ensure there is a populated interpolator.
        getInstance();

        return interpolator.interpolate(o);
    }

    /** Logs the JVM properties. */
    public static void logJvmProperties() {
        getInstance();
        log.info("jvm version:");
        log.info("  {}={}", JAVA_RUNTIME_NAME, instance.getString(JAVA_RUNTIME_NAME.property()));
        log.info("  {}={}", SUN_BOOT_LIBRARY_PATH,
            instance.getString(SUN_BOOT_LIBRARY_PATH.property()));
        log.info("  {}={}", JAVA_VM_VERSION, instance.getString(JAVA_VM_VERSION.property()));
    }

    /**
     * Returns a mutable configuration object as described in {@link #getInstance()}. This method
     * resets the immutable configuration in case a prior test forgot to call reset, so that
     * subsequent calls to {@link #getInstance()} will use this immutable instance rather than the
     * prior instance. For testing only. Production code should call {@link #getInstance()}.
     */
    public static synchronized Configuration getMutableInstance() {
        if (mutableInstance == null) {
            mutableInstance = getConfiguration();
            mutableInstance.setSynchronizer(new ReadWriteSynchronizer());
            instance = null;
        }
        return mutableInstance;
    }

    /** Clear the immutable and mutable configuration instances. */
    public static synchronized void reset() {
        instance = null;
        mutableInstance = null;
    }
}
