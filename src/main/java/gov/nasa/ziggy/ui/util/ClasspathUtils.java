package gov.nasa.ziggy.ui.util;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.ClasspathScanner;

/**
 * Classpath access utilities.
 *
 * @author Unknown Hero
 * @author Bill Wohler
 */
public class ClasspathUtils {
    private static final Logger log = LoggerFactory.getLogger(ClasspathUtils.class);

    private static final String PACKAGE_FILTER = "^gov\\.nasa\\..*";
    private static final String JAR_FILE_FILTER = "^.*-test-.*\\.jar";
    // module-info
    // org.glassfish.jaxb.runtime.v2.runtime.output.FastInfosetStreamWriterOutput

    private ClasspathUtils() {
    }

    /**
     * Scan for classes on the classpath that are Class.isAssignableFrom() the specified superclass.
     * This requires loading the Class objects for all classes found on the classpath that match the
     * package filters.
     * <p>
     * TODO Load the ClassFile objects into a Map, keyed by classname
     * <p>
     * Then use ClassFile.getSuperclass() and .getInterfaces() to provide the same functionality as
     * Class.isAssignableFrom without the overhead and unintended consequences of class-loading
     * every class on the classpath.
     */
    public static <E> Set<Class<? extends E>> scanFully(final Class<? extends E> superclass)
        throws Exception {
        final Set<Class<? extends E>> detectedClasses = new HashSet<>();

        ClasspathScanner classpathScanner = new ClasspathScanner();
        classpathScanner.addListener(classFile -> {
            // Ignore abstract and inner classes.
            if (classFile.isAbstract() || classFile.getName().contains("$")) {
                return;
            }
            try {
                @SuppressWarnings("unchecked")
                Class<? extends E> clazz = (Class<? extends E>) Class.forName(classFile.getName());

                if (superclass.isAssignableFrom(clazz)) {
                    log.debug("Found {}", clazz.getName());
                    detectedClasses.add(clazz);
                }
            } catch (Throwable ignore) {
                // There are a surprising number of classes on the classpath that you can't call
                // forName() on.
                log.debug("Class.forName failed: classFile={}", classFile.getName());
            }
        });
        classpathScanner.getIncludePackageFilters().add(PACKAGE_FILTER);
        classpathScanner.getExcludeJarFilters().add(JAR_FILE_FILTER);
        classpathScanner.scanForClasses();

        return detectedClasses;
    }

    /**
     * Scan for classes on the classpath that implement the specified interface. This method only
     * loads the Class objects if they implement the specified interface
     *
     * @param interfaceClass
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    public static <E> Set<Class<? extends E>> scanForInterfaceImpl(
        final Class<? extends E> interfaceClass) throws Exception {
        final Set<Class<? extends E>> detectedClasses = new HashSet<>();

        ClasspathScanner classpathScanner = new ClasspathScanner();
        classpathScanner.addListener(classFile -> {
            // Ignore abstract and inner classes.
            if (classFile.isAbstract() || classFile.getName().contains("$")) {
                return;
            }

            String[] interfaces = classFile.getInterfaces();
            for (String interfaceName : interfaces) {
                if (interfaceName.equals(interfaceClass.getName())) {
                    try {
                        Class<? extends E> clazz = (Class<? extends E>) Class
                            .forName(classFile.getName());
                        log.debug("Found {}", clazz);
                        detectedClasses.add(clazz);
                    } catch (ClassNotFoundException e) {
                        // This can never occur. The class was found on the classpath, hence it
                        // cannot be "not found."
                        throw new AssertionError(e);
                    }
                }
            }
        });
        classpathScanner.getIncludePackageFilters().add(PACKAGE_FILTER);
        classpathScanner.getExcludeJarFilters().add(JAR_FILE_FILTER);
        classpathScanner.scanForClasses();

        return detectedClasses;
    }
}
