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

    /**
     * The Log4j configuration file as a JVM argument.
     */
    public static String log4jConfigString() {
        return "-D" + PropertyName.LOG4J2_CONFIGURATION_FILE + "="
            + DirectoryProperties.ziggyHomeDir() + "/etc/log4j2.xml";
    }

    /**
     * The log file as a JVM argument.
     *
     * @see "etc/log4j2.xml"
     */
    public static String ziggyLog(String logFile) {
        return "-D" + PropertyName.ZIGGY_LOG_FILE + "=" + logFile;
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
}
