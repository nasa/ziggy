package gov.nasa.ziggy.services.database;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.DefaultConfiguration;
import org.apache.logging.log4j.core.config.xml.XmlConfigurationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.BuildInfo;
import gov.nasa.ziggy.util.os.OperatingSystemType;

/**
 * Provides initialization logic for java code called from MATLAB
 *
 * @author Todd Klaus
 */
public class MatlabJavaInitialization {

    private static final Logger log = LoggerFactory.getLogger(MatlabJavaInitialization.class);

    public static final String MATLAB_PIDS_FILENAME = ".matlab.pids";
    public static final String PID_FILE_CHARSET = "ISO-8859-1";

    private static final String MATLAB_LOG_FILE = DirectoryProperties.cliLogDir()
        .resolve("matlab.log")
        .toString();

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
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public static synchronized void initialize() {
        if (initialized) {
            return;
        }

        System.out.println("MatlabJavaInitialization: Initializing log4j with BasicConfigurator");

        Configurator.initialize(new DefaultConfiguration());

        log.info("Log4j initialized with BasicConfigurator, initializing Config service");

        ImmutableConfiguration config = ZiggyConfiguration.getInstance();

        if (config.getBoolean(PropertyName.MATLAB_LOG4J_CONFIG_INITIALIZE.property(), false)) {
            String log4jConfigFile = config
                .getString(PropertyName.LOG4J2_CONFIGURATION_FILE.property());

            log.info("{}={}", PropertyName.LOG4J2_CONFIGURATION_FILE, log4jConfigFile);

            if (log4jConfigFile != null) {
                log.info("Log4j initialized with DOMConfigurator from {}", log4jConfigFile);
                // TODO Evaluate setting of log4j property
                // If ZiggyConfiguration.getInstance() is called before we get there, this
                // statement will have no effect. Consider rearchitecting so that this property
                // is already set before the MATLAB binary is started, presuming this property
                // is even used.
                System.setProperty(PropertyName.ZIGGY_LOG_FILE.property(), MATLAB_LOG_FILE);
                ConfigurationFactory.setConfigurationFactory(new XmlConfigurationFactory());
                try {
                    Configurator.reconfigure(new URI(log4jConfigFile));
                } catch (URISyntaxException e) {
                    throw new PipelineException("Unable to configure Log4j", e);
                }
            }
        }

        log.info("Ziggy software version is {}", BuildInfo.ziggyVersion());
        log.info("Pipeline software version is {}", BuildInfo.pipelineVersion());
        ZiggyConfiguration.logJvmProperties();

        long pid = OperatingSystemType.newInstance().getProcInfo().getPid();
        log.info("Process ID is {}", pid);
        recordPid(pid);

        initialized = true;
    }

    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private static void recordPid(long pid) {
        String hostname = "<unknown>";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
            int dot = hostname.indexOf(".");
            if (dot != -1) {
                hostname = hostname.substring(0, dot);
            }
        } catch (UnknownHostException e) {
            // This can never happen. The localhost will be configured with
            // a name and address that can be retrieved.
            throw new AssertionError(e);
        }

        String PID_FILE = MATLAB_PIDS_FILENAME;

        File pidFile = new File(PID_FILE);
        try {
            FileUtils.writeStringToFile(pidFile, hostname + ":" + pid + "\n", PID_FILE_CHARSET);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to write to file " + PID_FILE, e);
        }
    }
}
