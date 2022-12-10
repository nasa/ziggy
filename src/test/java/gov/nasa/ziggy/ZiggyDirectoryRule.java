package gov.nasa.ziggy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.util.io.FileUtil;

/**
 * Implements a {@link TestRule} for creation of directories for use by unit tests.
 * <p>
 * The {@link ZiggyDirectoryRule} allows unit test classes to create clean directories for use by
 * their unit tests. Specifically, each unit test class gets its own subdirectory of build/test, and
 * within that directory each unit test gets its own subdirectory based on the test method name.
 * That directory is cleaned prior to test execution, but is left populated after test execution so
 * that the files created by the test are available for examination in the event of failures or
 * other problems.
 *
 * @author PT
 */
public class ZiggyDirectoryRule implements TestRule {

    private static final String BUILD_DIR_NAME = "build";
    private static final String TEST_DIR_NAME = "test";
    private static final String ZIGGY_PKG_PREFIX = "gov.nasa.ziggy.";

    private Path testDirPath;

    /**
     * Returns the {@link Path} of the directory for the current test method.
     */
    public Path testDirPath() {
        return testDirPath;
    }

    @Override
    public Statement apply(Statement statement, Description description) {

        // Create the build/test/<testClassName>/<methodName> path.
        String fullClassName = description.getTestClass().getCanonicalName();
        String className = fullClassName.startsWith(ZIGGY_PKG_PREFIX)
            ? fullClassName.substring(ZIGGY_PKG_PREFIX.length())
            : fullClassName;
        String methodName = description.getMethodName();
        testDirPath = Paths.get(BUILD_DIR_NAME, TEST_DIR_NAME, className, methodName);

        try {
            Files.createDirectories(testDirPath);

            // Clean the new directory prior to use.
            FileUtil.cleanDirectoryTree(testDirPath, true);

            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    statement.evaluate();
                }
            };
        } catch (IOException e) {
            throw new PipelineException("Unable to create directory " + testDirPath.toString(), e);
        }

    }

}
