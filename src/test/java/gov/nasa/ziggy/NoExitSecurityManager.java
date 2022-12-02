package gov.nasa.ziggy;

import static org.junit.Assert.assertEquals;

import java.security.Permission;

/**
 * Security manager that throws a SecurityException if System.exit() is called. This is used to
 * allow unit test execution to terminate properly in the event of a System.exit() call, and allows
 * the return code to be determined in this case. This is based on the NoExitSecurityManager in
 * <p>
 * https://github.com/stefanbirkner/system-rules/blob/master/src/main/java/org/junit/contrib
 * /java/lang/system/internal/NoExitSecurityManager.java
 * <p>
 * but without any of the additional contents of the package or the generality of that class: using
 * the entire package would have required a security analysis that is beyond the scope of this
 * effort.
 * <p>
 * USAGE:
 * <p>
 * In the test class, have a NoExitSecurityManager member that the setup method instantiates. Use
 * the System.securityManager() method to set the NoExitSecurityManager as the nominal one. Place
 * the code that is expected to use exit() in a try-catch block (note that after the code, in the
 * try block, you can put something like:
 *
 * <pre>
 * fail("System.exit() did not occur");
 * </pre>
 *
 * In the catch block, call the NoExitSecurityManager's assertExit() method to see that the expected
 * return code was used. Finally, in teardown, use the getOriginalSecurityManager() to recover the
 * security manager that was in use prior to the test and put it back in service.
 *
 * @author PT
 */
public class NoExitSecurityManager extends SecurityManager {

    private SecurityManager originalSecurityManager = null;
    private Integer exitCode = null;

    public NoExitSecurityManager() {
        originalSecurityManager = System.getSecurityManager();
    }

    public SecurityManager getOriginalSecurityManager() {
        return originalSecurityManager;
    }

    public Integer getExitCode() {
        return exitCode;
    }

    public void assertExit(int expectedCode) {
        assertEquals("Exit code of " + expectedCode + " not detected, value was " + exitCode,
            (long) expectedCode, (long) exitCode);
    }

    @Override
    public void checkExit(int status) {
        exitCode = status;
        throw new SecurityException();
    }

    /**
     * Defer any checkPermission calls to the original security manager, if any. Needed to allow the
     * tear down to execute.
     */
    @Override
    public void checkPermission(Permission p) {
        if (originalSecurityManager != null) {
            originalSecurityManager.checkPermission(p);
        }
    }

    /**
     * Defer any checkPermission calls to the original security manager, if any. Needed to allow the
     * tear down to execute.
     */
    @Override
    public void checkPermission(Permission p, Object o) {
        if (originalSecurityManager != null) {
            originalSecurityManager.checkPermission(p, o);
        }
    }

}
