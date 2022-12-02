package gov.nasa.ziggy.services.database;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.persistence.Embeddable;
import javax.persistence.Entity;
import javax.persistence.MappedSuperclass;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.util.ClasspathScanner;
import gov.nasa.ziggy.util.ClasspathScannerListener;
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
     *
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public Set<Class<?>> scanForClasses() throws Exception {
        log.debug("AnnotatedPojoList: Scanning class path for annotated classes");

        ClasspathScanner classpathScanner = new ClasspathScanner();
        classpathScanner.addListener(this);
        classpathScanner.setJarFilters(jarFilters);
        classpathScanner.setPackageFilters(packageFilters);
        classpathScanner.setClassPathToScan(classPathToScan);

        classpathScanner.scanForClasses();

        return detectedClasses;
    }

    /**
     * @throws Exception
     */
    @Override
    public void processClass(ClassFile classFile) throws Exception {
        if (isClassAnnotated(classFile)) {
            // log.debug("Found annotated class: " + className);
            Class<?> clazz = Class.forName(classFile.getName());
            detectedClasses.add(clazz);
        }
    }

    /**
     * Use the Javassist library to check the .class file for JPA annotations
     *
     * @param classFile
     * @return
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

    /**
     * @return the jarFilters
     */
    public Set<String> getJarFilters() {
        return jarFilters;
    }

    /**
     * @param jarFilters the jarFilters to set
     */
    public void setJarFilters(Set<String> jarFilters) {
        this.jarFilters = jarFilters;
    }

    /**
     * @return the packageFilters
     */
    public Set<String> getPackageFilters() {
        return packageFilters;
    }

    /**
     * @param packageFilters the packageFilters to set
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
