package gov.nasa.ziggy.util;

/**
 * Abstraction for {@link System#currentTimeMillis()} that allows tests to set an arbitrary value
 * for the time. This allows classes and methods to use {@link System#currentTimeMillis()} in normal
 * operation, but also allows the system time to be replaced by a user-specified time for unit
 * testing of those same classes and methods.
 *
 * @author PT
 */
public class SystemTime {

    /**
     * User-set value for time. This is used for testing and debugging purposes only.
     */
    private static long userSetTime = 0L;

    /**
     * Returns the user-set time value, if any, and resets the user-set time to zero. If no user-set
     * time value is present (indicated by a zero value), the value of
     * {@link System#currentTimeMillis()} is returned.
     *
     * @return user-set time or system time.
     */
    public static long currentTimeMillis() {
        long timeValue = userSetTime == 0L ? System.currentTimeMillis() : userSetTime;
        userSetTime = 0L;
        return timeValue;
    }

    /**
     * Allows the user to set a time value to be returned. This is a single-use value: once it is
     * returned, the user value is deleted and any subsequent calls to {@link #currentTimeMillis()}
     * will return the value of {@link System#currentTimeMillis()}.
     *
     * @param userTime time that the user would like the system to return.
     */
    public static void setUserTime(long userTime) {
        userSetTime = userTime;
    }
}
