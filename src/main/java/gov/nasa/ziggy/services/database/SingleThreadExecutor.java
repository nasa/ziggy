package gov.nasa.ziggy.services.database;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Class that provides a new instance of a single-thread executor. It can also be configured to
 * return a user-supplied executor, which supports testing of methods that include use of a single
 * thread executor.
 *
 * @author PT
 */
public class SingleThreadExecutor {

    public static ExecutorService instance = null;

    /**
     * @return static {@link ExecutorService} instance if defined, otherwise a new
     * {@link ExecutorService} instance obtained from {@link Executors#newSingleThreadExecutor()}.
     */
    public static ExecutorService newInstance() {
        ExecutorService service = instance;
        if (service == null) {
            service = Executors.newSingleThreadExecutor();
        }
        return service;
    }

    /**
     * Sets an instance of {@link ExecutorService} as the static instance for the class. This allows
     * a mocked instance to be returned to any and all callers.
     */
    public static void setInstance(ExecutorService staticInstance) {
        instance = staticInstance;
    }

    /**
     * Deletes the static {@link ExecutorService} instance for this class.
     */
    public static void reset() {
        instance = null;
    }
}
