package gov.nasa.ziggy.ui.common;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.util.ClasspathScanner;

public class ClasspathUtils {
    private static final Logger log = LoggerFactory.getLogger(ClasspathUtils.class);

    private String packageFilters = "gov.nasa";

    public ClasspathUtils() {
    }

    /**
     * Scan for classes on the classpath that are Class.isAssignableFrom() the specified superclass.
     * This requires loading the Class objects for all classes found on the classpath that match the
     * package filters.
     * <p>
     * TODO: load the ClassFile objects into a Map, keyed by classname. Then use
     * ClassFile.getSuperclass() and .getInterfaces() to provide the same functionality as
     * Class.isAssignableFrom without the overhead and unintended consequences of class-loading
     * every class on the classpath.
     *
     * @param superclass
     * @return
     * @throws Exception
     */
    public <E> Set<Class<? extends E>> scanFully(final Class<? extends E> superclass)
        throws Exception {
        final Set<Class<? extends E>> detectedClasses = new HashSet<>();

        ClasspathScanner classpathScanner = new ClasspathScanner();
        classpathScanner.addListener(classFile -> {
            if (classFile.isAbstract() || classFile.getName().contains("$")) {
                // ignore abstract and inner classes
                return;
            }
            try {
                @SuppressWarnings("unchecked")
                Class<? extends E> clazz = (Class<? extends E>) Class.forName(classFile.getName());

                if (superclass.isAssignableFrom(clazz)) {
                    log.debug("found: " + clazz.getName());
                    detectedClasses.add(clazz);
                }
            } catch (Throwable ignore) {
            }
        });
        classpathScanner.getPackageFilters().add(packageFilters);
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
    public <E> Set<Class<? extends E>> scanForInterfaceImpl(final Class<? extends E> interfaceClass)
        throws Exception {
        final Set<Class<? extends E>> detectedClasses = new HashSet<>();

        ClasspathScanner classpathScanner = new ClasspathScanner();
        classpathScanner.addListener(classFile -> {
            String[] interfaces = classFile.getInterfaces();
            for (String interfaceName : interfaces) {
                if (interfaceName.equals(interfaceClass.getName())) {
                    @SuppressWarnings("unchecked")
                    Class<? extends E> detectedClass = (Class<? extends E>) Class
                        .forName(classFile.getName());
                    detectedClasses.add(detectedClass);
                }
            }
        });
        classpathScanner.getPackageFilters().add(packageFilters);
        classpathScanner.scanForClasses();

        return detectedClasses;
    }

    public String getPackageFilters() {
        return packageFilters;
    }

    public void setPackageFilters(String packageFilters) {
        this.packageFilters = packageFilters;
    }
}
