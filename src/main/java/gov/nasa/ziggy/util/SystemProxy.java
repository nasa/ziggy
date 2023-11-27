package gov.nasa.ziggy.util;

/**
 * Delegator for calls to select {@link System} methods. This is used to allow production code to
 * call the actual System method, but also allows test code to disable or modify the results of the
 * calls. This is necessary because you really don't want test code to call production code that
 * calls {@link System#exit(int)} for real, as that will at the very least kill the test process.
 *
 * @author PT
 */
public class SystemProxy {

    /**
     * User-set value for time. This is used for testing and debugging purposes only.
     */
    private static long userSetTime = 0L;

    /**
     * Determines whether {@link System#exit(int)} will actually be called (true == called, false ==
     * don't actually call exit).
     */
    private static boolean exitEnabled = true;

    /**
     * Captures the latest value of the {@link #exit(int)} argument. This value can only be
     * retrieved once, because when it is retrieved the value is nullified.
     */
    private static Integer latestExitCode;

    /**
     * Returns the user-set time value, if any, and resets the user-set time to zero. If no user-set
     * time value is present (indicated by a zero value), the value of
     * {@link System#currentTimeMillis()} is returned.
     *
     * @return user-set time or system time
     */
    public static long currentTimeMillis() {
        long timeValue = userSetTime == 0L ? System.currentTimeMillis() : userSetTime;
        userSetTime = 0L;
        return timeValue;
    }

    /**
     * Manages the exit process. If {@link #exitEnabled} has been set to false, the exit will not
     * occur.
     * <p>
     * Note that each call to {@link #disableExit()} only prevents the first call to
     * {@link System#exit(int)} that follows the call to {@link #disableExit()}. This ensures that
     * the system does not remain indefinitely in the state with exit disabled.
     */
    public static void exit(int exitCode) {
        if (exitEnabled) {
            System.exit(exitCode);
        }
        latestExitCode = exitCode;
        exitEnabled = true;
    }

    /**
     * Allows the user to set a time value to be returned. This is a single-use value: once it is
     * returned, the user value is deleted and any subsequent calls to {@link #currentTimeMillis()}
     * will return the value of {@link System#currentTimeMillis()}.
     *
     * @param userTime time that the user would like the system to return
     */
    public static void setUserTime(long userTime) {
        userSetTime = userTime;
    }

    /**
     * Prevents {@link System#exit(int)} from being called.
     * <p>
     * Note that {@link #disableExit()} is a one-time use method, in that the first use of
     * {@link #exit(int)} after the call to {@link #disableExit()} will re-enable calls to the
     * system exit method. Hence the recommended use of this method is to call it from a unit test's
     * {@link Before} method.
     */
    public static void disableExit() {
        exitEnabled = false;
    }

    /**
     * Returns the most recent value of the exit code argument for {@link #exit(int)}, and then
     * clears its value (i.e., the next call to {@link #getLatestExitCode()} will return null).
     */
    public static Integer getLatestExitCode() {
        Integer exitCode = latestExitCode;
        latestExitCode = null;
        return exitCode;
    }
}
