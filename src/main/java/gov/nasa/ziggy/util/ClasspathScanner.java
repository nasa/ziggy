package gov.nasa.ziggy.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
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

    private Set<String> includeJarFilters = new HashSet<>();
    private Set<String> excludeJarFilters = new HashSet<>();
    private Set<String> includePackageFilters = new HashSet<>();
    private Set<String> excludePackageFilters = new HashSet<>();
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

    private void notifyListeners(ClassFile classFile) {
        for (ClasspathScannerListener listener : listeners) {
            listener.processClass(classFile);
        }
    }

    /**
     * For each element in the classpath, scan the contents (either a recursive directory search or
     * a JAR scan) for annotated classes.
     */
    public void scanForClasses() {
        log.debug("ClasspathScanner: Scanning class path for matching classes");

        visitedClassPathElements = new HashSet<>();
        Set<String> classPath = classPathToScan;

        if (classPath == null || classPath.isEmpty()) {
            // No user-specified classpath provided, so parse the actual classpath.
            classPath = parseClassPath();
        }

        scanClassPath(classPath);
    }

    /**
     * Scan the specified class path for annotated classes.
     */
    private void scanClassPath(Set<String> classPath) {
        for (String classPathElement : classPath) {
            if (!visitedClassPathElements.contains(classPathElement)) {
                visitedClassPathElements.add(classPathElement);

                File classPathElementFile = new File(classPathElement);
                if (classPathElementFile.exists()) {
                    if (classPathElementFile.isDirectory()) {
                        log.debug("Scanning directory {}" + classPathElementFile);
                        scanDirectory(classPathElementFile, classPathElementFile);
                    } else if (classPathElementFile.getName().endsWith(".jar")) {
                        if (matchesJarFilter(classPathElementFile.getName())) {
                            log.debug("Scanning JAR file {}", classPathElementFile);
                            scanJar(classPathElementFile);
                        } else {
                            log.debug("Skipping JAR file {} because it does not match filters",
                                classPathElementFile);
                        }
                    }
                }
            }
        }
    }

    /**
     * Recursively scan a directory for classes.
     */
    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    private void scanDirectory(File rootDirectory, File directory) {
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
                        log.debug("Processing file={}", file.getName());
                        FileInputStream fis;
                        try {
                            fis = new FileInputStream(file);
                            processFile(fis);
                        } catch (FileNotFoundException e) {
                            // Can never occur. The FileInputStream opens on a file that
                            // was obtained from listing files in a directory.
                            throw new AssertionError(e);
                        }
                    }
                }
            }
        }
    }

    /**
     * Scan the contents of a jar file for classes.
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private void scanJar(File jarFile) {
        try (JarFile jar = new JarFile(jarFile)) {
            Enumeration<JarEntry> entries = jar.entries();

            // First, check to see if the MANIFEST has a Class-Path entry.
            Manifest manifest = jar.getManifest();
            if (manifest != null) {
                log.debug("Scanning MANIFEST for {}", jarFile.getName());
                Attributes mainAttrs = manifest.getMainAttributes();
                if (mainAttrs != null) {
                    String manifestClassPath = mainAttrs.getValue(Attributes.Name.CLASS_PATH);
                    if (manifestClassPath != null) {
                        log.debug("Found MANIFEST ClassPath={}", manifestClassPath);

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
                    log.debug("Processing entry={}", entry);
                    processFile(jar.getInputStream(entry));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to scan file " + jarFile.toString(), e);
        }
    }

    /**
     * Load a .class file (from a file or jar) and notify the listeners
     */
    private void processFile(InputStream is) {
        ClassFile classFile = createClassFile(is);

        notifyListeners(classFile);
    }

    /**
     * Use the javassist library to read the contents of the .class file
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
     */
    private Set<String> parseClassPath() {
        Set<String> classPath = new HashSet<>();
        ClassLoader classLoader = getClass().getClassLoader();

        if (classLoader instanceof URLClassLoader) {
            log.debug("classLoader={} instanceof URLClassLoader", classLoader);
            URL[] urls = ((URLClassLoader) classLoader).getURLs();
            for (URL url : urls) {
                String filename = url.getFile();
                log.debug("Adding url={}", filename);
                classPath.add(filename);
            }
        } else {
            log.debug("Parsing {}", PropertyName.JAVA_CLASS_PATH);
            StringTokenizer st = new StringTokenizer(
                ZiggyConfiguration.getInstance().getString(PropertyName.JAVA_CLASS_PATH.property()),
                File.pathSeparator);
            while (st.hasMoreTokens()) {
                String filename = st.nextToken();
                log.debug("Adding file={}", filename);
                classPath.add(filename);
            }
        }
        return classPath;
    }

    private String convertPathToPackageName(String path) {
        return path.replace(File.separatorChar, '.');
    }

    /**
     * Check whether the className matches the jarFilters.
     */
    private boolean matchesJarFilter(String jarFileName) {
        return matchesFilters(jarFileName, includeJarFilters, excludeJarFilters);
    }

    /**
     * Check whether the className matches the packageFilters.
     */
    private boolean matchesPackageFilter(String className) {
        return matchesFilters(className, includePackageFilters, excludePackageFilters);
    }

    private boolean matchesFilters(String s, Set<String> includeFilters,
        Set<String> excludeFilters) {
        boolean matches = false;
        if (includeFilters.isEmpty()) {
            matches = true;
        } else {
            for (String filter : includeFilters) {
                if (s.matches(filter)) {
                    matches = true;
                    break;
                }
            }
        }
        if (matches) {
            for (String filter : excludeFilters) {
                if (s.matches(filter)) {
                    return false;
                }
            }
        }

        return matches;
    }

    public Set<String> getIncludeJarFilters() {
        return includeJarFilters;
    }

    /**
     * Only jars that match the given non-null set of regular expressions will be processed. If this
     * is not set, all jar files will be processed. Use {@link #setExcludeJarFilters(Set)} to reduce
     * this set.
     */
    public void setIncludeJarFilters(Set<String> includeJarFilters) {
        checkNotNull(includeJarFilters, "includeJarFilters");
        this.includeJarFilters = includeJarFilters;
    }

    public Set<String> getExcludeJarFilters() {
        return excludeJarFilters;
    }

    /**
     * Jars that match the given non-null set of regular expressions will not be processed.
     */
    public void setExcludeJarFilters(Set<String> excludeJarFilters) {
        checkNotNull(excludeJarFilters, "excludeJarFilters");
        this.excludeJarFilters = excludeJarFilters;
    }

    public Set<String> getIncludePackageFilters() {
        return includePackageFilters;
    }

    /**
     * Only packages that match the given non-null set of regular expressions will be processed. If
     * this is not set, all packages will be processed. Use {@link #setExcludePackageFilters(Set)}
     * to reduce this set.
     */
    public void setIncludePackageFilters(Set<String> includePackageFilters) {
        checkNotNull(includePackageFilters, "includePackageFilters");
        this.includePackageFilters = includePackageFilters;
    }

    public Set<String> getExcludePackageFilters() {
        return excludePackageFilters;
    }

    /**
     * Packages that match the given non-null set of regular expressions will not be processed.
     */
    public void setExcludePackageFilters(Set<String> excludePackageFilters) {
        checkNotNull(includePackageFilters, "includePackageFilters");
        this.excludePackageFilters = excludePackageFilters;
    }

    public Set<String> getClassPathToScan() {
        return classPathToScan;
    }

    public void setClassPathToScan(Set<String> classPathToScan) {
        this.classPathToScan = classPathToScan;
    }
}
