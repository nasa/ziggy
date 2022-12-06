package gov.nasa.ziggy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;

import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.services.database.TestUtils;
import gov.nasa.ziggy.util.io.FileUtil;

/**
 * Base class for all Ziggy unit test classes.
 * <p>
 * The {@link ZiggyUnitTest} provides the following services to its subclasses:
 * <ol>
 * <li>A master setup method that is invoked prior to each test's execution. This method ensures
 * that the build/test directory is present, sets system properties that the tests require, and then
 * executes any setup methods that the subclass requires.
 * <li>A master teardown method that is invoked subsequent to each test's execution. This method
 * tears down any test database, cleans out the build/test directory, and clears any system
 * properties set for the subclass. It also executes any teardown methods that the subclass
 * requires.
 * <li>Abstract methods that subclasses can override to supply their own specialized setup and
 * teardown methods.
 * <li>An abstract method that provides system properties for each concrete subclass.
 * </ol>
 *
 * @author PT
 */
public abstract class ZiggyUnitTest {

    /**
     * System properties that must be set prior to test and cleared after each test. The static
     * field provides "memory" from one test class to another, such that a given test class can
     * clear the properties set by the previous test class.
     */
    private static Map<String, String> currentSystemProperties = new HashMap<>();

    /**
     * Location for all temporary files and directories.
     */
    public static final String BUILD_DIR = "build";
    public static final String TEST_DIR = "test";
    public static final File BUILD_TEST_FILE = new File(BUILD_DIR, TEST_DIR);
    public static final Path BUILD_TEST_PATH = Paths.get(BUILD_DIR, TEST_DIR);

    /**
     * {@link Map} of system properties required by a test class, where the keys are the property
     * names and the values are the property values. subclasses must override this method to ensure
     * that properties are correctly set and cleared.
     *
     * @return {@link Map} of properties required by a given test class.
     */
    public Map<String, String> systemProperties() {
        return new HashMap<>();
    }

    /**
     * Setups that are specific to a given test class. Subclasses must override this method to
     * ensure that setups are properly executed.
     *
     * @throws Exception to ensure that any checked exception in any concrete version of this method
     * is properly represented in the throws clause.
     */
    public void setUp() throws Exception {
    }

    /**
     * Teardowns that are specific to a given test class. Subclasses must override this method to
     * ensure that teardowns are properly executed.
     *
     * @throws Exception to ensure that any checked exception in any concrete version of this method
     * is properly represented in the throws clause.
     */
    public void tearDown() throws Exception {
    }

    /**
     * Clean up for test classes. Clears properties, tears down the database, resets the
     * {@link DatabaseService}, and cleans the build/test directory.
     * <p>
     * Note that this method is called at by the {@link #masterTearDown()} method (as you would
     * expect), but also at the start of the {@link #masterSetUp()} method (as you might not
     * expect). This ensures that if the test class that executed prior to the current one
     * experienced a problem that caused its teardown process to fail, any subsequent test class
     * will nonetheless clean up after its predecessor.
     *
     * @throws IOException if thrown from the {@link FileUtil#cleanDirectoryTree(Path)} method.
     */
    final void cleanUp() throws IOException {
        for (String propertyName : currentSystemProperties.keySet()) {
            System.clearProperty(propertyName);
        }
        currentSystemProperties = new HashMap<>();

        // If the database was instantiated, tear it down now.
        if (System.getProperty("hibernate.dialect") != null) {
            TestUtils.tearDownDatabase();
        }
        DatabaseService.reset();

        // Clean out the build/test directory
        if (Files.isDirectory(BUILD_TEST_PATH)) {
            FileUtil.cleanDirectoryTree(BUILD_TEST_PATH);
        }
    }

    @Before
    public final void masterSetUp() throws Exception {

        // Create the build/test directory, if necessary.
        BUILD_TEST_FILE.mkdirs();
        cleanUp();
        currentSystemProperties = systemProperties();
        for (Map.Entry<String, String> entry : currentSystemProperties.entrySet()) {
            System.setProperty(entry.getKey(), entry.getValue());
        }

        setUp();
    }

    @After
    public final void masterTearDown() throws Exception {
        tearDown();
        cleanUp();
    }
}
