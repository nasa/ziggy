package gov.nasa.ziggy.services.process;

import org.apache.commons.lang3.StringUtils;

import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;

/**
 * Provides common utilities needed by multiple different external process subsystems.
 *
 * @author PT
 */
public class ExternalProcessUtils {

    private static final String SUPERVISOR_LOG_FILE_NAME = "supervisor.log";

    /**
     * The Log4j configuration file as a JVM argument.
     */
    public static String log4jConfigString() {
        return "-D" + PropertyName.LOG4J2_CONFIGURATION_FILE + "="
            + DirectoryProperties.ziggyHomeDir() + "/etc/log4j2.xml";
    }

    /**
     * The Java library path as a JVM argument.
     */
    public static String javaLibraryPath() {
        return "-Djava.library.path=" + bareJavaLibraryPath();
    }

    public static String jnaLibraryPath() {
        return "-Djna.library.path=" + bareJavaLibraryPath();
    }

    /**
     * The Java library path without its JVM argument decorations. If the pipeline has defined a
     * library path, that path will be first, followed by the Ziggy library path; if no pipeline
     * library path is present, the Ziggy library path will be returned.
     */
    private static String bareJavaLibraryPath() {
        String pipelineLibPath = ZiggyConfiguration.getInstance()
            .getString(PropertyName.LIBPATH.property(), null);
        return StringUtils.isEmpty(pipelineLibPath) ? DirectoryProperties.ziggyLibDir().toString()
            : pipelineLibPath + ":" + DirectoryProperties.ziggyLibDir().toString();
    }

    /**
     * The prefix to be used for the supervisor log file (i.e., it goes into logs/supervisor).
     */
    public static String supervisorLogPrefix() {
        return "-Dlog4j.logfile.prefix=" + DirectoryProperties.supervisorLogDir().toString();
    }

    /**
     * The log file name used by the supervisor (hence also by the workers).
     *
     * @return
     */
    public static String supervisorLogFilename() {
        return DirectoryProperties.supervisorLogDir().resolve(SUPERVISOR_LOG_FILE_NAME).toString();
    }
}
