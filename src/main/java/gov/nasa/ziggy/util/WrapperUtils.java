package gov.nasa.ziggy.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.exec.CommandLine;

public class WrapperUtils {

    public static final String WRAPPER_LIBRARY_PATH_PROP_NAME_PREFIX = "wrapper.java.library.path.";
    public static final String WRAPPER_JAVA_ADDITIONAL_PROP_NAME_PREFIX = "wrapper.java.additional.";
    public static final String WRAPPER_APP_PARAMETER_PROP_NAME_PREFIX = "wrapper.app.parameter.";
    public static final String WRAPPER_CLASSPATH_PROP_NAME_PREFIX = "wrapper.java.classpath.";
    public static final String WRAPPER_LOG_FILE_PROP_NAME = "wrapper.logfile";

    public static final String COMPRESSED_OOPS_VALUE = "-XX:+UseCompressedOops";
    public static final String OMIT_STACK_TRACE_VALUE = "-XX:-OmitStackTraceInFastThrow";

    // Do not allow JIT to optimize the AlgorithmStateFiles methods. If it does, it will
    // assume that the population of status files inspected by AlgorithmStateFiles is fixed
    // and it will never detect changes to that population (i.e., it won't see any further
    // subtask progress).
    public static final String NO_JIT_ALGORITHM_STATE_FILES_VALUE = "-XX:CompileCommand=exclude,gov/nasa/ziggy/pipeline/step/AlgorithmStateFiles,*";

    /** Commands that the wrapper command accepts. */
    public enum WrapperCommand {
        START, STOP, STATUS;

        @Override
        public String toString() {
            return name().toLowerCase();
        }
    }

    /**
     * Returns a wrapper parameter string such as wrapper.app.parameter=-Dfoo.
     *
     * @param wrapperPropName wrapper parameter
     * @param value the value of the parameter
     */
    public static String wrapperParameter(String wrapperPropName, String value) {
        checkNotNull(wrapperPropName, "wrapperPropName");
        checkArgument(!wrapperPropName.isBlank(), "wrapperPropName can't be empty");
        checkNotNull(value, "value");
        checkArgument(!value.isBlank(), "value can't be empty");

        StringBuilder s = new StringBuilder();
        s.append(wrapperPropName).append("=").append(value);
        return s.toString();
    }

    /**
     * Returns a wrapper parameter string such as wrapper.app.parameter.1=-Dfoo.
     *
     * @param wrapperPropNamePrefix wrapper parameter prefix
     * @param index counter to append to parameter prefix
     * @param value the value of the parameter
     */
    public static String wrapperParameter(String wrapperPropNamePrefix, int index, String value) {
        checkNotNull(wrapperPropNamePrefix, "wrapperPropNamePrefix");
        checkArgument(!wrapperPropNamePrefix.isBlank(), "wrapperPropNamePrefix can't be empty");
        checkNotNull(value, "value");
        checkArgument(!value.isBlank(), "value can't be empty");
        checkArgument(index >= 0, "index must be non-negative");

        StringBuilder s = new StringBuilder();
        s.append(wrapperPropNamePrefix).append(index).append("=").append(value);
        return s.toString();
    }

    /**
     * Adds a collection of statically-defined wrapper properties with a given property base name.
     *
     * @return number of properties added.
     */
    public static int addWrapperStaticProperties(CommandLine commandLine, String propertyBaseName,
        Collection<String> staticPropertyValues) {
        Iterator<String> propertyValueIterator = staticPropertyValues.iterator();
        int propCounter = 0;
        while (propertyValueIterator.hasNext()) {
            commandLine.addArgument(
                wrapperParameter(propertyBaseName, ++propCounter, propertyValueIterator.next()));
        }
        return staticPropertyValues.size();
    }
}
