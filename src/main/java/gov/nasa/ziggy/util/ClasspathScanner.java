package gov.nasa.ziggy.util;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javassist.bytecode.ClassFile;

/**
 * Scan the classpath for all classes that match the supplied filters and notify the
 * {@link ClasspathScannerListener}s. Dives into Jar files and supports jar filename and package
 * name filters
 *
 * @author Todd Klaus
 */
public class ClasspathScanner {
    private static final Logger log = LoggerFactory.getLogger(ClasspathScanner.class);

    private Set<String> jarFilters = new HashSet<>();
    private Set<String> packageFilters = new HashSet<>();
    private Set<String> classPathToScan = new HashSet<>();
    private Set<String> visitedClassPathElements = new HashSet<>();

    private final List<ClasspathScannerListener> listeners = new LinkedList<>();

    public ClasspathScanner() {
    }

    public boolean addListener(ClasspathScannerListener listener) {
        return listeners.add(listener);
    }

    public boolean removeListener(ClasspathScannerListener listener) {
        return listeners.remove(listener);
    }

    private void notifyListeners(ClassFile classFile) throws Exception {
        for (ClasspathScannerListener listener : listeners) {
            listener.processClass(classFile);
        }
    }

    /**
     * For each element in the classpath, scan the contents (either a recursive directory search or
     * a JAR scan) for annotated classes.
     *
     * @throws Exception
     */
    public void scanForClasses() throws Exception {
        log.debug("ClasspathScanner: Scanning class path for matching classes");

        visitedClassPathElements = new HashSet<>();
        Set<String> classPath = classPathToScan;

        if (classPath == null || classPath.isEmpty()) {
            // no user-specified classpath provided, so parse the actual
            // classpath
            classPath = parseClassPath();
        }

        scanClassPath(classPath);
    }

    /**
     * Scan the specified class path for annotated classes
     *
     * @param classPath
     * @param detectedClasses
     * @return
     * @throws Exception
     */
    private void scanClassPath(Set<String> classPath) throws Exception {
        for (String classPathElement : classPath) {
            if (!visitedClassPathElements.contains(classPathElement)) {
                visitedClassPathElements.add(classPathElement);

                File classPathElementFile = new File(classPathElement);
                if (classPathElementFile.exists()) {
                    if (classPathElementFile.isDirectory()) {
                        log.debug("scanning directory: " + classPathElementFile);
                        scanDirectory(classPathElementFile, classPathElementFile);
                    } else if (classPathElementFile.getName().endsWith(".jar")) {
                        if (matchesJarFilter(classPathElementFile.getName())) {
                            log.debug("scanning JAR file: " + classPathElementFile);
                            scanJar(classPathElementFile);
                        } else {
                            log.debug("skipping JAR file because it does not match filters: "
                                + classPathElementFile);
                        }
                    }
                }
            }
        }
    }

    /**
     * Recursively scan a directory for classes
     *
     * @param classPathElementFile
     * @param detectedClasses
     * @throws Exception
     */
    private void scanDirectory(File rootDirectory, File directory) throws Exception {
        String directoryName = rootDirectory.getAbsolutePath();
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    scanDirectory(rootDirectory, file);
                } else {
                    String relativePath = file.getAbsolutePath()
                        .substring(directoryName.length() + 1);
                    String fullyQualifiedName = convertPathToPackageName(relativePath);
                    if (matchesPackageFilter(fullyQualifiedName)
                        && fullyQualifiedName.endsWith(".class")) {
                        log.debug("processing file: " + file.getName());
                        FileInputStream fis = new FileInputStream(file);
                        processFile(fis);
                    }
                }
            }
        }
    }

    /**
     * Scan the contents of a jar file for classes
     *
     * @param classPathElementFile
     * @param detectedClasses
     * @throws Exception
     */
    private void scanJar(File jarFile) throws Exception {
        try (JarFile jar = new JarFile(jarFile)) {
            Enumeration<JarEntry> entries = jar.entries();

            // first check to see if the MANIFEST has a Class-Path entry
            Manifest manifest = jar.getManifest();
            if (manifest != null) {
                log.debug("scanning MANIFEST for: " + jarFile.getName());
                Attributes mainAttrs = manifest.getMainAttributes();
                if (mainAttrs != null) {
                    String manifestClassPath = mainAttrs.getValue(Attributes.Name.CLASS_PATH);
                    if (manifestClassPath != null) {
                        log.debug("Found MANIFEST Class-Path");

                        String classPathRelativeDir = jarFile.getParentFile().getAbsolutePath();
                        String[] classPathEntries = manifestClassPath.split("\\s+");
                        Set<String> classPath = new HashSet<>();

                        for (String classPathEntrie : classPathEntries) {
                            File classPathElementFile = new File(
                                classPathRelativeDir + File.separator + classPathEntrie);
                            if (matchesJarFilter(classPathElementFile.getName())) {
                                classPath.add(classPathElementFile.getAbsolutePath());
                            }
                        }
                        scanClassPath(classPath);
                    }
                }
            }

            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String fullyQualifiedName = convertPathToPackageName(entry.getName());
                if (matchesPackageFilter(fullyQualifiedName)
                    && fullyQualifiedName.endsWith(".class")) {
                    log.debug("processing jar entry: " + entry);
                    InputStream is = jar.getInputStream(entry);
                    processFile(is);
                }
            }
        }
    }

    /**
     * Load a .class file (from a file or jar) and notify the listeners
     *
     * @param is
     * @param detectedClasses
     * @throws Exception
     */
    private void processFile(InputStream is) throws Exception {
        ClassFile classFile = createClassFile(is);

        notifyListeners(classFile);
    }

    /**
     * Use the javassist library to read the contents of the .class file
     *
     * @param is
     * @return
     */
    private ClassFile createClassFile(InputStream is) {
        ClassFile cf = null;
        try (DataInputStream dstream = new DataInputStream(new BufferedInputStream(is))) {
            cf = new ClassFile(dstream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return cf;
    }

    /**
     * Parse the contents of the classpath
     *
     * @return
     */
    private Set<String> parseClassPath() {
        Set<String> classPath = new HashSet<>();
        ClassLoader classLoader = getClass().getClassLoader();

        if (classLoader instanceof URLClassLoader) {
            log.debug("classLoader instanceof URLClassLoader");
            URL[] urls = ((URLClassLoader) classLoader).getURLs();
            for (URL url : urls) {
                String filename = url.getFile();
                log.debug("adding url:" + filename);
                classPath.add(filename);
            }
        } else {
            log.debug("parsing java.class.path");
            StringTokenizer st = new StringTokenizer(SystemUtils.JAVA_CLASS_PATH,
                File.pathSeparator);
            while (st.hasMoreTokens()) {
                String filename = st.nextToken();
                log.debug("adding file:" + filename);
                classPath.add(filename);
            }
        }
        return classPath;
    }

    private String convertPathToPackageName(String path) {
        return path.replace(File.separatorChar, '.');
    }

    /**
     * Check whether the className matches the jarFilters
     *
     * @param className
     * @return
     */
    private boolean matchesJarFilter(String jarFileName) {
        if (jarFilters == null || jarFilters.isEmpty()) {
            return true;
        }

        for (String filter : jarFilters) {
            if (jarFileName.startsWith(filter)) {
                return true;
            }
        }

        // log.debug("skipping jar: " + jarFileName);
        return false;
    }

    /**
     * Check whether the className matches the packageFilters
     *
     * @param className
     * @return
     */
    private boolean matchesPackageFilter(String className) {
        if (packageFilters == null || packageFilters.isEmpty()) {
            return true;
        }

        for (String filter : packageFilters) {
            if (className.startsWith(filter)) {
                return true;
            }
        }

        // log.debug("skipping package: " + className);
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

    /**
     * @return the classPathToScan
     */
    public Set<String> getClassPathToScan() {
        return classPathToScan;
    }

    /**
     * @param classPathToScan the classPathToScan to set
     */
    public void setClassPathToScan(Set<String> classPathToScan) {
        this.classPathToScan = classPathToScan;
    }
}
