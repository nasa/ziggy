package gov.nasa.ziggy.services.database;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;

/**
 * Runs the {@link DatabaseTransaction#transaction()} method of a {@link DatabaseTransaction} within
 * a new transaction context. Use static method {@link #performTransaction(DatabaseTransaction)} to
 * run {@link DatabaseTransaction#transaction()} in the current thread or
 * {@link #performTransactionInThread(DatabaseTransaction)} to run
 * {@link DatabaseTransaction#transaction()} in a new thread.
 * <p>
 * The following example runs code in a transaction context within the current thread and doesn't
 * return a value.
 *
 * <pre>
 * DatabaseTransactionFactory.performTransaction(() -&#62; {
 *     // Content of transaction
 *     return null;
 * });
 * </pre>
 *
 * The following example returns a value from a separate thread.
 *
 * <pre>
 * Integer result = (Integer) DatabaseTransactionFactory.performTransactionInThread(() -&#62; {
 *     // Content of transaction
 *     return queryResult;
 * });
 * </pre>
 *
 * In all cases, the steps executed are as follows:
 * <ol>
 * <li>An instance of {@link DatabaseService} is obtained and the transaction is started.
 * <li>The contents of the {@link DatabaseTransaction#transaction()} are executed.
 * <li>The transaction is committed.
 * <li>If an exception occurred during the transaction, a catch block is executed. This includes any
 * optional content provided as part of {@link DatabaseTransaction#catchBlock(Throwable)}. The
 * transaction is rolled back. The catch block ends by throwing a {@link PipelineException} unless
 * {@link DatabaseTransaction#swallowException()} returns true.
 * <li>In a finally block, the database session is closed and any contents of
 * {@link DatabaseTransaction#finallyBlock()} are executed.
 * </ol>
 *
 * @see DatabaseTransaction
 * @author Miles Cote
 * @author PT
 * @author Bill Wohler
 */
public class DatabaseTransactionFactory<T> implements Callable<T> {

    private static final Logger log = LoggerFactory.getLogger(DatabaseTransactionFactory.class);

    private DatabaseTransaction<T> transactionInstance;

    private DatabaseTransactionFactory(DatabaseTransaction<T> transactionInstance) {
        this.transactionInstance = transactionInstance;
    }

    /**
     * Executes a transaction in the calling thread. The user must supply an instance of
     * {@link DatabaseTransaction} with a concrete {@link DatabaseTransaction#transaction()}. Any
     * returns from {@link DatabaseTransaction#transaction()} are returned by this method.
     */
    public static Object performTransaction(DatabaseTransaction<?> transactionInstance) {
        return new DatabaseTransactionFactory<>(transactionInstance).performTransactionInternal();
    }

    /**
     * Executes a transaction in a new thread. The user must supply an instance of
     * {@link DatabaseTransaction} with a concrete {@link DatabaseTransaction#transaction()}. Any
     * returns from {@link DatabaseTransaction#transaction()} are returned by this method.
     */
    public static Object performTransactionInThread(DatabaseTransaction<?> transactionInstance) {
        return new DatabaseTransactionFactory<>(transactionInstance)
            .performTransactionInThreadInternal();
    }

    /**
     * Allows the transaction to be called within a thread.
     */
    @Override
    public T call() {
        return performTransactionInternal();
    }

    /**
     * Performs the transaction in a template method that supplies the database service operations
     * that are needed to perform a complete, encapsulated transaction. See the class documentation
     * for examples.
     *
     * @see #transaction()
     * @see #catchBlock(Throwable)
     * @see #finallyBlock()
     * @see #swallowException()
     */
    private T performTransactionInternal() {
        T result = null;
        DatabaseService databaseService = DatabaseService.getInstance();
        try {
            if (!transactionInstance.silent()) {
                log.info("Beginning transaction");
            }
            databaseService.beginTransaction();
            result = transactionInstance.transaction();
            databaseService.commitTransaction();
            if (!transactionInstance.silent()) {
                log.info("Transaction completed");
            }
        } catch (Throwable e) {
            log.error("Exception during transaction", e);
            databaseService.rollbackTransactionIfActive();
            transactionInstance.catchBlock(e);
            if (!transactionInstance.swallowException()) {
                throw new PipelineException("Transaction failed with error.", e);
            }
        } finally {
            databaseService.closeCurrentSession();
            transactionInstance.finallyBlock();
        }

        return result;
    }

    /**
     * Performs the transaction in a separate thread. See the class documentation for examples.
     *
     * @see #performTransactionInternal()
     */
    private T performTransactionInThreadInternal() {
        ExecutorService executorService = SingleThreadExecutor.newInstance();
        Future<T> future = executorService.submit(this);
        executorService.shutdown();

        try {
            return future.get();
        } catch (Exception e) {
            throw new PipelineException("Unable to retrieve value from future", e);
        }
    }

}
