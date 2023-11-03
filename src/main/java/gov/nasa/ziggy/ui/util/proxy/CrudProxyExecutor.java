package gov.nasa.ziggy.ui.util.proxy;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.services.database.DatabaseTransaction;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.services.database.SingleThreadExecutor;
import gov.nasa.ziggy.ui.UiDatabaseException;
import gov.nasa.ziggy.ui.util.models.DatabaseModelRegistry;

/**
 * Contains a single-thread {@link ExecutorService} used by the UI code to invoke the CrudProxy
 * classes. This ensures that all Hibernate calls are made from the same thread, and therefore the
 * same Hibernate Session
 *
 * @author Todd Klaus
 */
public class CrudProxyExecutor {
    private static final Logger log = LoggerFactory.getLogger(CrudProxyExecutor.class);

    private static final CrudProxyExecutor instance = new CrudProxyExecutor();
    private final ExecutorService executor = SingleThreadExecutor.newInstance();

    public CrudProxyExecutor() {
    }

    /**
     * For tasks that return a result (of type T)
     *
     * @param <T>
     * @param task
     * @return
     */
    public static <T> T executeSynchronous(Callable<T> task) {
        Future<T> result = instance.executor.submit(task);

        try {
            return result.get();
        } catch (Exception e) {
            instance.handleErrorSynchronous();
            throw new UiDatabaseException(e.getCause());
        }
    }

    /**
     * For tasks that return void
     *
     * @param task
     */
    public static void executeSynchronous(Runnable task) {
        Future<?> result = instance.executor.submit(task);

        try {
            result.get();
        } catch (Exception e) {
            instance.handleErrorSynchronous();
            throw new UiDatabaseException(e.getCause());
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T executeSynchronousDatabaseTransaction(Callable<T> task) {
        return (T) DatabaseTransactionFactory.performTransaction(new DatabaseTransaction<T>() {
            @Override
            public void catchBlock(Throwable e) {
                DatabaseModelRegistry.invalidateModels();
            }

            @Override
            public T transaction() throws Exception {
                return task.call();
            }
        });
    }

    /**
     * Error handler. Close the current Hibernate Session and notify all models. The error handler
     * actions execute synchronously in the executor's thread.
     */
    private void handleErrorSynchronous() {
        executeSynchronous(() -> {
            handleErrorInternal();
        });
    }

    /**
     * Error handler. Close the current Hibernate Session and notify all models. Can be executed
     * synchronously or as part of an asynchronous operation.
     */
    private void handleErrorInternal() {
        DatabaseService databaseService = DatabaseService.getInstance();
        log.info("Handling a proxy error, closing session and invalidating all registered models");
        databaseService.closeCurrentSession();
        DatabaseModelRegistry.invalidateModels();
    }
}
