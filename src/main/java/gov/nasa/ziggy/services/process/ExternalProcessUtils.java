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
     * The Java library path as a JVM argument.
     */
    public static String javaLibraryPath() {
        return "-D" + PropertyName.JAVA_LIBRARY_PATH + "=" + bareJavaLibraryPath();
    }

    /**
     * The Java library path without its JVM argument decorations. If the pipeline has defined a
     * library path, that path will be first, followed by the Ziggy library path; if no pipeline
     * library path is present, the Ziggy library path will be returned.
     */
    private static String bareJavaLibraryPath() {
        String pipelineLibPath = ZiggyConfiguration.getInstance()
            .getString(PropertyName.LIBPATH.property(), null);
        return StringUtils.isBlank(pipelineLibPath) ? DirectoryProperties.ziggyLibDir().toString()
            : pipelineLibPath + ":" + DirectoryProperties.ziggyLibDir().toString();
    }
}
