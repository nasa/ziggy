package gov.nasa.ziggy.ui.ops.parameters;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.ui.common.ClasspathUtils;

/**
 * Cache that holds {@link ClassWrapper} objects for all implementations of the {@link Parameters}
 * interface found on the classpath.
 *
 * @author Todd Klaus
 */
public class ParametersClassCache {
    private static List<ClassWrapper<Parameters>> cache = new LinkedList<>();
    private static boolean initialized = false;

    private ParametersClassCache() {
    }

    /**
     * Return a cached List of all classes that implement the {@link Parameters} interface found on
     * the classpath.
     *
     * @return
     * @throws Exception
     */
    public static synchronized List<ClassWrapper<Parameters>> getCache() throws Exception {
        if (!initialized) {
            initializeCache();
        }

        return cache;
    }

    /**
     * Return a cached List of all classes that implement the {@link Parameters} interface and are
     * sub-classes for the specified filter class found on the classpath.
     *
     * @param filter
     * @return
     * @throws Exception
     */
    public static synchronized List<ClassWrapper<Parameters>> getCache(
        Class<? extends Parameters> filter) throws Exception {
        if (!initialized) {
            initializeCache();
        }

        List<ClassWrapper<Parameters>> filteredCache = new LinkedList<>();

        for (ClassWrapper<Parameters> classWrapper : cache) {
            if (filter.isAssignableFrom(classWrapper.getClazz())) {
                filteredCache.add(classWrapper);
            }
        }

        return filteredCache;
    }

    private static synchronized void initializeCache() throws Exception {
        cache = new LinkedList<>();

        ClasspathUtils classpathUtils = new ClasspathUtils();
        Set<Class<? extends Parameters>> detectedClasses = classpathUtils
            .scanForInterfaceImpl(Parameters.class);

        for (Class<? extends Parameters> clazz : detectedClasses) {
            ClassWrapper<Parameters> wrapper = new ClassWrapper<>(clazz);
            cache.add(wrapper);
        }

        Collections.sort(cache);
    }
}
