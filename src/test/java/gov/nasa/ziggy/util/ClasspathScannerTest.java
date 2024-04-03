package gov.nasa.ziggy.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

public class ClasspathScannerTest {

    private static final String ZIGGY_JAR_REGEXP = "ziggy-[\\d.]+\\.jar";

    private Path ziggyJarFile = findZiggyJar();
    private ClasspathScannerListener classpathScannerListener;
    private boolean called;
    private Set<String> classes = new HashSet<>();

    @Before
    public void setUp() {
        called = false;
        classes.clear();
    }

    @Test
    public void testListener() {
        ClasspathScanner classpathScanner = classpathScanner();

        // addListener() called by classpathScanner().
        classpathScanner.scanForClasses();
        assertTrue(called);
        called = false;

        classpathScanner.removeListener(classpathScannerListener);
        classpathScanner.scanForClasses();
        assertFalse(called);
    }

    @Test
    public void testIncludeJarFilters() {
        ClasspathScanner classpathScanner = classpathScanner();
        Set<String> includeJarFilters = Set.of(ZIGGY_JAR_REGEXP);
        classpathScanner.setIncludeJarFilters(includeJarFilters);
        assertEquals(includeJarFilters, classpathScanner.getIncludeJarFilters());
        classpathScanner.scanForClasses();
        checkForZiggyClasses();
    }

    @Test
    public void testIncludeNonexistentJarFilters() {
        ClasspathScanner classpathScanner = classpathScanner();
        classpathScanner.setIncludeJarFilters(Set.of("i-dont-exist"));
        classpathScanner.scanForClasses();
        assertTrue(classes.isEmpty());
    }

    @Test
    public void testExcludeJarFilters() {
        ClasspathScanner classpathScanner = classpathScanner();
        Set<String> excludeJarFilters = Set.of(ZIGGY_JAR_REGEXP);
        classpathScanner.setExcludeJarFilters(excludeJarFilters);
        assertEquals(excludeJarFilters, classpathScanner.getExcludeJarFilters());
        classpathScanner.scanForClasses();
        assertTrue(classes.isEmpty());
    }

    @Test
    public void testExcludeNonexistentJarFilters() {
        ClasspathScanner classpathScanner = classpathScanner();
        classpathScanner.setExcludeJarFilters(Set.of("i-dont-exist"));
        classpathScanner.scanForClasses();
        checkForZiggyClasses();
    }

    @Test
    public void testIncludePackageFilters() {
        ClasspathScanner classpathScanner = classpathScanner();
        Set<String> includePackageFilters = Set.of("gov\\.nasa\\.ziggy\\.util\\..*");
        classpathScanner.setIncludePackageFilters(includePackageFilters);
        assertEquals(includePackageFilters, classpathScanner.getIncludePackageFilters());
        classpathScanner.scanForClasses();
        checkForClassesInPackage("gov.nasa.ziggy.util");
    }

    @Test
    public void testIncludeNonexistentPackageFilters() {
        ClasspathScanner classpathScanner = classpathScanner();
        Set<String> includePackageFilters = Set.of("foo.bar.baz");
        classpathScanner.setIncludePackageFilters(includePackageFilters);
        classpathScanner.scanForClasses();
        assertTrue(classes.isEmpty());
    }

    @Test
    public void testExcludePackageFilters() {
        ClasspathScanner classpathScanner = classpathScanner();
        Set<String> excludePackageFilters = Set.of("gov\\.nasa\\.ziggy\\..*");
        classpathScanner.setExcludePackageFilters(excludePackageFilters);
        assertEquals(excludePackageFilters, classpathScanner.getExcludePackageFilters());
        classpathScanner.scanForClasses();
        assertTrue(classes.isEmpty());
    }

    @Test
    public void testExcludeNonexistentPackageFilters() {
        ClasspathScanner classpathScanner = classpathScanner();
        Set<String> excludePackageFilters = Set.of("foo.bar.baz");
        classpathScanner.setExcludePackageFilters(excludePackageFilters);
        assertEquals(excludePackageFilters, classpathScanner.getExcludePackageFilters());
        classpathScanner.scanForClasses();
        checkForZiggyClasses();
    }

    private ClasspathScanner classpathScanner() {
        ClasspathScanner classpathScanner = new ClasspathScanner();
        Set<String> classPathToScan = Set.of(ziggyJarFile.toString());
        classpathScanner.setClassPathToScan(classPathToScan);
        assertEquals(classPathToScan, classpathScanner.getClassPathToScan());
        classpathScannerListener = classpathScannerListener();
        classpathScanner.addListener(classpathScannerListener);

        return classpathScanner;
    }

    private ClasspathScannerListener classpathScannerListener() {
        return classFile -> {
            // System.out.println(classFile.getName());

            called = true;
            classes.add(classFile.getName());
        };
    }

    // Returns the path to build/libs/ziggy-m.n.p.jar.
    private Path findZiggyJar() {
        try {
            List<Path> paths = Files.list(Paths.get("build/libs"))
                .filter(path -> path.getFileName().toString().matches(ZIGGY_JAR_REGEXP))
                .collect(Collectors.toList());
            if (paths.size() != 1) {
                throw new IllegalStateException(
                    "Could not find one match of ziggy-[\\d.]+\\.jar in build/libs: " + paths);
            }
            return paths.get(0);
        } catch (IOException e) {
            throw new IllegalStateException("Can't open build/libs");
        }
    }

    private void checkForZiggyClasses() {
        checkForClassesInPackage("gov.nasa.ziggy");
    }

    private void checkForClassesInPackage(String packageName) {
        assertTrue(classes.size() > 0);

        for (String clazz : classes) {
            // Avoid unnecessary string concatenation in assertTrue(message, condition) call.
            if (!clazz.startsWith(packageName)) {
                fail(clazz + " doesn't start with " + packageName);
            }
        }
    }
}
