package gov.nasa.ziggy.services.database;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;

/**
 * Runs the {@link DatabaseTransaction#transaction()} method of a {@link DatabaseTransaction} within
 * a new transaction context. Use the static method
 * {@link #performTransaction(DatabaseTransaction)}.
 * <p>
 * The following example runs code in a transaction context and doesn't return a value.
 *
 * <pre>
 * DatabaseTransactionFactory.performTransaction(() -&#62; {
 *     // Content of transaction
 *     return null;
 * });
 * </pre>
 *
 * The following example returns a value.
 *
 * <pre>
 * Integer result = (Integer) DatabaseTransactionFactory.performTransaction(() -&#62; {
 *     // Content of transaction
 *     return queryResult;
 * });
 * </pre>
 *
 * In all cases, the steps executed are as follows:
 * <ol>
 * <li>An instance of {@link DatabaseService} is obtained and the transaction is started.
 * <li>The contents of the {@link DatabaseTransaction#transaction()} are executed.
 * <li>The transaction is committed and the session is closed.
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
public class DatabaseTransactionFactory<T> {

    /**
     * Specifies the transaction context. Specifically, specifies whether the call to
     * {@link DatabaseTransactionFactory#performTransaction(DatabaseTransaction) occurs in the
     * context of an existing transaction. If the context is an existing transaction, then
     * performTransaction should not begin, commit, or roll back the transaction, or close the
     * session at the end of the operation; if the context is outside of an existing transaction,
     * then the {@link TransactionContext} must provide all of the aforementioned functions.
     *
     * @author PT
     */
    private enum TransactionContext {

        /**
         * Existing transaction, so don't do anything to break the transaction that is outside of
         * the control of the current activities.
         */
        IN_EXISTING_TRANSACTION {
            @Override
            public void beginTransaction() {
            }

            @Override
            public void commitTransaction() {
            }

            @Override
            public void rollBackTransactionIfActive() {
            }

            @Override
            public void closeCurrentSession() {
            }
        },

        /**
         * No pre-existing transaction, so one must be created and managed.
         */
        IN_NEW_TRANSACTION {
            @Override
            public void beginTransaction() {
                DatabaseService.getInstance().beginTransaction();
            }

            @Override
            public void commitTransaction() {
                DatabaseService.getInstance().commitTransaction();
            }

            @Override
            public void rollBackTransactionIfActive() {
                DatabaseService.getInstance().rollbackTransactionIfActive();
            }

            @Override
            public void closeCurrentSession() {
                DatabaseService.getInstance().closeCurrentSession();
            }
        };

        public abstract void beginTransaction();

        public abstract void commitTransaction();

        public abstract void rollBackTransactionIfActive();

        public abstract void closeCurrentSession();
    }

    private static final Logger log = LoggerFactory.getLogger(DatabaseTransactionFactory.class);

    private DatabaseTransaction<T> transactionInstance;
    private TransactionContext transactionContext;
    private DatabaseService databaseService;

    private DatabaseTransactionFactory(DatabaseTransaction<T> transactionInstance) {
        this.transactionInstance = transactionInstance;
    }

    /**
     * Executes a transaction in the calling thread. The user must supply an instance of
     * {@link DatabaseTransaction} with a concrete {@link DatabaseTransaction#transaction()}. Any
     * returns from {@link DatabaseTransaction#transaction()} are returned by this method.
     */
    public static Object performTransaction(DatabaseTransaction<?> transactionInstance) {
        if (log.isDebugEnabled()) {
            log.debug("Called by {}", caller());
        }
        return new DatabaseTransactionFactory<>(transactionInstance).performTransactionInternal();
    }

    /**
     * Returns the class, method, and line number of the stack element (or elements if a nested
     * transaction occurs) just above this class in the call stack as a non-null string suitable for
     * inclusion in a log message. Enable the trace logging level to get a stack backtrace as well.
     */
    private static String caller() {
        StackTraceElement[] stackTrace = new Exception().getStackTrace();

        // Display the full stack if the trace log level is enabled.
        if (log.isTraceEnabled()) {
            for (int i = 0; i < stackTrace.length; i++) {
                log.info("{}: {}.{}: {}", i, shortClassName(stackTrace[i].getClassName()),
                    stackTrace[i].getMethodName(), Integer.toString(stackTrace[i].getLineNumber()));
            }
        }

        boolean appendNextElement = false;
        String caller = "";
        for (StackTraceElement stackTraceElement : stackTrace) {
            if (DatabaseTransactionFactory.class.getCanonicalName()
                .equals(stackTraceElement.getClassName())) {
                appendNextElement = true;
            } else if (appendNextElement) {
                appendNextElement = false;
                String location = shortClassName(stackTraceElement.getClassName()) + "."
                    + stackTraceElement.getMethodName() + ": "
                    + Integer.toString(stackTraceElement.getLineNumber());
                caller = caller.isEmpty() ? location : caller + ", " + location;
            }
        }
        return caller.isEmpty() ? "unknown" : caller;
    }

    private static String shortClassName(String canonicalName) {
        return canonicalName.substring(canonicalName.lastIndexOf(".") + 1);
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
        databaseService = DatabaseService.getInstance();
        transactionContext = databaseService.transactionIsActive()
            ? TransactionContext.IN_EXISTING_TRANSACTION
            : TransactionContext.IN_NEW_TRANSACTION;
        log.debug("session={}, transactionIsActive={}, thread={}", databaseService.getSession(),
            databaseService.transactionIsActive(), Thread.currentThread().getName());
        if (transactionContext == TransactionContext.IN_EXISTING_TRANSACTION
            && !transactionInstance.allowExistingTransaction()) {
            throw new PipelineException(
                "Attempted to perform database access in existing transaction");
        }
        T result = null;
        try {
            transactionContext.beginTransaction();
            result = transactionInstance.transaction();
            transactionContext.commitTransaction();
        } catch (Throwable e) {
            transactionContext.rollBackTransactionIfActive();
            transactionInstance.catchBlock(e);
            if (!transactionInstance.swallowException()) {
                throw new PipelineException("Transaction failed with error", e);
            }
            log.error("Exception during transaction", e);
        } finally {
            transactionContext.closeCurrentSession();
            transactionInstance.finallyBlock();
        }
        return result;
    }

    /** For testing only. */
    static String callerTest() {
        return caller();
    }
}
