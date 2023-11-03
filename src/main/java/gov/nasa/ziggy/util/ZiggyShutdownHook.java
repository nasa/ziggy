package gov.nasa.ziggy.util;

/**
 * Provides an abstraction of the {@link Runtime#addShutdownHook(Thread)} functionality for Ziggy.
 * In addition to simple addition of shutdown hooks to the runtime environment, the following
 * additional functionality is provided:
 * <ol>
 * <li>Allows the caller to only add a shutdown hook if the JVM is not in the process of shutting
 * down.
 * <li>Allows the caller to block execution if the JVM is in the process of shutting down.
 * </ol>
 * Note that the extra functionality will only work correctly if all shutdown hooks are added using
 * the {@link ZiggyShutdownHook}. This is because the use of this shutdown hook abstraction allows
 * Ziggy to detect whether one of the {@link ZiggyShutdownHook} shutdown hooks has been executed.
 *
 * @author PT
 */
public class ZiggyShutdownHook {

    private static boolean shutdownInProgress = false;

    /**
     * Determines whether a shutdown is in progress.
     *
     * @return true if JVM is currently shutting down, false otherwise.
     */
    public static boolean shutdownInProgress() {
        return shutdownInProgress;
    }

    private static synchronized void markShutdownHasBegun() {
        shutdownInProgress = true;
    }

    /**
     * Adds a shutdown hook if and only if the JVM is not in the process of shutting down.
     *
     * @param shutdownHookRunnable {@link Runnable} that contains the content of the shutdown hook.
     */
    public static void addShutdownHook(Runnable shutdownHookRunnable) {
        if (!shutdownInProgress()) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                markShutdownHasBegun();
                shutdownHookRunnable.run();
            }));
        }
    }

    /**
     * Blocks execution of the calling thread if a JVM shutdown is in progress.
     */
    public static void blockIfShuttingDown() {
        if (shutdownInProgress()) {
            while (true) {
            }
        }
    }
}
