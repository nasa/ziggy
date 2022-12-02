package gov.nasa.ziggy.services.database;

import java.io.File;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.DefaultConfiguration;
import org.apache.logging.log4j.core.config.xml.XmlConfigurationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.util.ZiggyBuild;
import gov.nasa.ziggy.util.os.OperatingSystemType;

/**
 * Provides initialization logic for java code called from MATLAB
 *
 * @author Todd Klaus
 */
public class MatlabJavaInitialization {
    private static final Logger log = LoggerFactory.getLogger(MatlabJavaInitialization.class);

    /**
     * Property in the config service that points to the log4j.xml file used by Java code called
     * from MATLAB
     */

    public static final String LOG4J_MATLAB_CONFIG_FILE_PROP = "matlab.log4j.config";
    public static final String LOG4J_MATLAB_CONFIG_INITIALIZE_PROP = "matlab.log4j.initialize";
    public static final String LOG4J_LOGFILE_PREFIX = "log4j.logfile.prefix";
    public static final String MATLAB_PIDS_FILENAME = ".matlab.pids";

    private static final String DEFAULT_LOG4J_LOGFILE_PREFIX = "${ziggy.config.dir}/../logs/matlab";
    public static final String PID_FILE_CHARSET = "ISO-8859-1";

    private static boolean initialized = false;

    /**
     * Initialize log4j and the Config service for Java code called from MATLAB.
     * <p>
     * We have a bootstrapping problem here. We'd like to have a config property that points to the
     * log4j.xml file that the MATLAB/Java code should use, but we would also like to have logging
     * configured before we initialize the Config service so that we can see that the config is
     * coming from the correct source. So, we follow this sequence:
     * <ol>
     * <li>Initialize log4j with the BasicConfigurator (just log to console, which gets captured by
     * the java worker process that launched MATLAB)
     * <li>Initialize the config service
     * <li>Re-initialize log4j with the config service property
     * </ol>
     *
     * @throws PipelineException
     */
    public static synchronized void initialize() {
        if (!initialized) {
            System.out
                .println("MatlabJavaInitialization: Initializing log4j with BasicConfigurator");

            Configurator.initialize(new DefaultConfiguration());

            log.info("Log4j initialized with BasicConfigurator, initializing Config service");

            Configuration config = ZiggyConfiguration.getInstance();

            if (config.getBoolean(LOG4J_MATLAB_CONFIG_INITIALIZE_PROP, false)) {
                String log4jConfigFile = config.getString(LOG4J_MATLAB_CONFIG_FILE_PROP);

                log.info(LOG4J_MATLAB_CONFIG_FILE_PROP + " = " + log4jConfigFile);

                if (log4jConfigFile != null) {
                    log.info("Log4j initialized with DOMConfigurator from: " + log4jConfigFile);
                    System.setProperty(LOG4J_LOGFILE_PREFIX, DEFAULT_LOG4J_LOGFILE_PREFIX);
                    ConfigurationFactory.setConfigurationFactory(new XmlConfigurationFactory());
                    try {
                        Configurator.reconfigure(new URI(log4jConfigFile));
                    } catch (URISyntaxException e) {
                        throw new PipelineException("Unable to configure Log4j", e);
                    }
                }
            }

            log.info("jvm version:");
            log.info("  java.runtime.name=" + System.getProperty("java.runtime.name"));
            log.info("  sun.boot.library.path=" + System.getProperty("sun.boot.library.path"));
            log.info("  java.vm.version=" + System.getProperty("java.vm.version"));

            ZiggyBuild.logVersionInfo(log);

            try {
                long pid = OperatingSystemType.getInstance().getProcInfo().getPid();
                log.info("process ID: " + pid);
                recordPid(pid);
            } catch (Throwable t) {
                log.warn("Unable to get process ID: " + t);
            }

            initialized = true;
        }
    }

    private static void recordPid(long pid) throws Exception {
        String hostname = "<unknown>";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
            int dot = hostname.indexOf(".");
            if (dot != -1) {
                hostname = hostname.substring(0, dot);
            }
        } catch (Exception e) {
            log.warn("failed to get hostname", e);
        }

        String PID_FILE = MATLAB_PIDS_FILENAME;

        File pidFile = new File(PID_FILE);
        FileUtils.writeStringToFile(pidFile, hostname + ":" + pid + "\n", PID_FILE_CHARSET);
    }
}
