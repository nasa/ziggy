package gov.nasa.ziggy.services.database;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.ClasspathScanner;
import gov.nasa.ziggy.util.ClasspathScannerListener;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Entity;
import jakarta.persistence.MappedSuperclass;
import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.ClassFile;

/**
 * Scan the classpath for all classes annotated with {@link Entity} or {@link Embeddable} so they
 * can programatically be added to the Hibernate AnnotatedConfiguration.
 * <p>
 * Dives into Jar files and supports jar filename and package name filters
 * <p>
 * Uses Javassist to read the bytecode to avoid forcing the classloader to load every class on the
 * classpath.
 *
 * @author Todd Klaus
 */
public class AnnotatedPojoList implements ClasspathScannerListener {
    private static final Logger log = LoggerFactory.getLogger(AnnotatedPojoList.class);

    private Set<String> jarFilters = new HashSet<>();
    private Set<String> packageFilters = new HashSet<>();
    private Set<String> classPathToScan = new HashSet<>();

    private final Set<Class<?>> detectedClasses = new HashSet<>();

    public AnnotatedPojoList() {
    }

    /**
     * For each element in the classpath, scan the contents (either a recursive directory search or
     * a JAR scan) for annotated classes.
     */
    public Set<Class<?>> scanForClasses() {
        log.debug("Scanning class path for annotated classes");

        ClasspathScanner classpathScanner = new ClasspathScanner();
        classpathScanner.addListener(this);
        classpathScanner.setIncludeJarFilters(jarFilters);
        classpathScanner.setIncludePackageFilters(packageFilters);
        classpathScanner.setClassPathToScan(classPathToScan);

        classpathScanner.scanForClasses();

        return detectedClasses;
    }

    @Override
    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    public void processClass(ClassFile classFile) {
        if (isClassAnnotated(classFile)) {
            // log.debug("Found annotated class: " + className);
            try {
                detectedClasses.add(Class.forName(classFile.getName()));
            } catch (ClassNotFoundException e) {
                // Can never occur. The class name comes from a class on the classpath.
                throw new AssertionError(e);
            }
        }
    }

    /**
     * Use the Javassist library to check the .class file for JPA annotations
     */
    private boolean isClassAnnotated(ClassFile classFile) {
        // TODO: do we care about global metadata?
//        if (cf.getName().endsWith(".package-info")) {
//            int idx = cf.getName().indexOf(".package-info");
//            String pkgName = cf.getName().substring(0, idx);
//            log.info("found package: " + pkgName);
//            packages.add(pkgName);
//            continue;
//        }

        AnnotationsAttribute visible = (AnnotationsAttribute) classFile
            .getAttribute(AnnotationsAttribute.visibleTag);
        if (visible != null) {
            boolean isEntity = visible.getAnnotation(Entity.class.getName()) != null;
            if (isEntity) {
                log.debug("found @Entity: " + classFile.getName());
                return true;
            }
            boolean isEmbeddable = visible.getAnnotation(Embeddable.class.getName()) != null;
            if (isEmbeddable) {
                log.debug("found @Embeddable: " + classFile.getName());
                return true;
            }
            boolean isEmbeddableSuperclass = visible
                .getAnnotation(MappedSuperclass.class.getName()) != null;
            if (isEmbeddableSuperclass) {
                log.debug("found @MappedSuperclass: " + classFile.getName());
                return true;
            }
        }
        return false;
    }

    public Set<String> getJarFilters() {
        return jarFilters;
    }

    /**
     * Only jars that match the given set of regular expressions will be processed. If this is not
     * set, all jar files will be processed.
     */
    public void setJarFilters(Set<String> jarFilters) {
        this.jarFilters = jarFilters;
    }

    public Set<String> getPackageFilters() {
        return packageFilters;
    }

    /**
     * Only packages that match the given set of regular expressions will be processed. If this is
     * not set, all packages will be processed.
     */
    public void setPackageFilters(Set<String> packageFilters) {
        this.packageFilters = packageFilters;
    }

    public Set<String> getClassPathToScan() {
        return classPathToScan;
    }

    public void setClassPathToScan(Set<String> classPathToScan) {
        this.classPathToScan = classPathToScan;
    }
}
