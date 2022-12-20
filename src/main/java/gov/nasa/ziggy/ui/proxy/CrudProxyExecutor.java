package gov.nasa.ziggy.ui.proxy;

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
import gov.nasa.ziggy.ui.models.DatabaseModelRegistry;

/**
 * Contains a single-thread {@link ExecutorService} used by the UI code to invoke the CrudProxy
 * classes. This ensures that all hibernate calls are made from the same thread, and therefore the
 * same hibernate Session
 *
 * @author Todd Klaus
 */
public class CrudProxyExecutor {
    private static final Logger log = LoggerFactory.getLogger(CrudProxyExecutor.class);

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
    public <T> T executeSynchronous(Callable<T> task) {
        Future<T> result = executor.submit(task);

        try {
            return result.get();
        } catch (Exception e) {
            handleErrorSynchronous();
            throw new UiDatabaseException(e.getCause());
        }
    }

    /**
     * For tasks that return void
     *
     * @param task
     */
    public void executeSynchronous(Runnable task) {
        Future<?> result = executor.submit(task);

        try {
            result.get();
        } catch (Exception e) {
            handleErrorSynchronous();
            throw new UiDatabaseException(e.getCause());
        }
    }

    public <T> T executeSynchronousDatabaseTransaction(Callable<T> task) {
        return executeSynchronousDatabaseTransaction(task, false);
    }

    @SuppressWarnings("unchecked")
    public <T> T executeSynchronousDatabaseTransaction(Callable<T> task, boolean silent) {
        return (T) DatabaseTransactionFactory
            .performTransactionInThread(new DatabaseTransaction<T>() {
                @Override
                public boolean silent() {
                    return silent;
                }

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
