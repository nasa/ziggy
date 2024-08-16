package gov.nasa.ziggy.services.database;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;

/**
 * Provides database access to Ziggy classes.
 * <p>
 * {@link DatabaseOperations} subclasses can use the
 * {@link #performTransaction(NonReturningDatabaseTransaction)} method to execute a transaction that
 * doesn't return anything; they can also use the
 * {@link #performTransaction(ReturningDatabaseTransaction)} to execute a transaction that does
 * return a value. The performTransaction method provides the transaction boundaries, i.e., a new
 * transaction begins at start of method execution and the resulting session is closed at the end of
 * the transaction.
 * <p>
 * The following example runs code in a transaction context and doesn't return a value.
 *
 * <pre>
 * performTransaction(() -&#62; {
 *     // Content of transaction
 * });
 * </pre>
 *
 * The following example returns a value.
 *
 * <pre>
 * int result = performTransaction(() -&#62; {
 *     // Content of transaction
 *     return queryResult;
 * });
 * </pre>
 *
 * In all cases, the steps executed are as follows:
 * <ol>
 * <li>An instance of {@link DatabaseService} is obtained and the transaction is started.
 * <li>The contents of the transaction are executed.
 * <li>The transaction is committed and the session is closed.
 * <li>If an exception occurred during the transaction, a catch block is executed. This includes any
 * optional content provided as part of {@link DatabaseTransactionFeatures#catchBlock(Throwable)}.
 * The transaction is rolled back. The catch block ends by throwing a {@link PipelineException}
 * unless {@link DatabaseTransactionFeatures#swallowException()} returns true.
 * <li>In a finally block, the database session is closed and any contents of
 * {@link DatabaseTransactionFeatures#finallyBlock()} are executed.
 * </ol>
 * <p>
 * Any class that needs to perform database transactions must extend this class. It is strongly
 * recommended that only the *Operations classes extend it, and that classes that require database
 * access use methods in the *Operations classes.
 *
 * @see DatabaseTransactionFeatures
 * @author PT
 */
public abstract class DatabaseOperations {

    public enum TransactionContext {

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

    private static final Logger log = LoggerFactory.getLogger(DatabaseOperations.class);

    private DatabaseService databaseService;

    /**
     * Performs a database transaction that returns a value.
     *
     * @param <T> Class of the returned value.
     */
    public final <T> T performTransaction(ReturningDatabaseTransaction<T> transaction) {
        return performTransactionInternal(transaction);
    }

    /** Performs a database transaction that returns no value. */
    public final void performTransaction(NonReturningDatabaseTransaction transaction) {
        ReturningDatabaseTransaction<Void> returningTransactionInstance = new ReturningDatabaseTransaction<>() {

            @Override
            public void catchBlock(Throwable e) {
                transaction.catchBlock(e);
            }

            @Override
            public void finallyBlock() {
                transaction.finallyBlock();
            }

            @Override
            public boolean swallowException() {
                return transaction.swallowException();
            }

            @Override
            public boolean allowExistingTransaction() {
                return transaction.allowExistingTransaction();
            }

            @Override
            public Void transaction() throws Exception {
                transaction.transaction();
                return null;
            }
        };
        performTransactionInternal(returningTransactionInstance);
    }

    /**
     * Performs the transaction in a template method that supplies the database service operations
     * that are needed to perform a complete, encapsulated transaction. See the class documentation
     * for examples.
     *
     * @param <T> Class of object returned from the transaction.
     * @see #transaction()
     * @see #catchBlock(Throwable)
     * @see #finallyBlock()
     * @see #swallowException()
     */
    @SuppressWarnings("unchecked")
    private <T> T performTransactionInternal(ReturningDatabaseTransaction<?> transactionInstance) {
        databaseService = DatabaseService.getInstance();
        TransactionContext transactionContext = databaseService.transactionIsActive()
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
            result = (T) transactionInstance.transaction();
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

    /**
     * Provides support methods that can be overridden, and which provide additional features for a
     * database transaction:
     * <ol>
     * <li>{@link #catchBlock(Throwable)} provides additional functionality that is exercised in the
     * catch block of the transaction wrapper; default is no functionality.
     * <li>{@link #finallyBlock()} provides additional functionality that is exercised in the
     * finally block of the transaction wrapper; default is no functionality.
     * <li>{@link #swallowException()} returns a boolean that indicates whether an exception in the
     * try block (i.e., during the transaction} is swallowed (if the method returns true), or if a
     * {@link PipelineException} is thrown at the end of the catch block (if the method returns
     * false). Default is false (i.e., at the end of the catch block throw another exception).
     * </ol>
     *
     * @author PT
     */
    private interface DatabaseTransactionFeatures {

        /**
         * Provides the capability to execute code in the catch block after the transaction is
         * rolled back. Override to implement such a block.
         */
        default void catchBlock(Throwable e) {
        }

        /**
         * Provides the capability to execute code in the finally block after the session is closed.
         * Override to implement such a block.
         */
        default void finallyBlock() {
        }

        /**
         * Determines whether an exception (specifically, a {@link PipelineException}) is thrown at
         * the end of the catch block. The default behavior is that an exception is thrown, which
         * will generally result in processing coming to a halt. Override in cases in which
         * processing should continue normally despite the failure of the transaction.
         */
        default boolean swallowException() {
            return false;
        }

        /**
         * Determines whether a transaction may be performed in the context of an existing
         * transaction. In some cases this is not desirable, as transactions performed in that
         * context will not commit when the transaction completes (they won't commit until the outer
         * transaction completes). If {@link #allowExistingTransaction()} returns false, a
         * {@link PipelineException} will be thrown if a transaction occurs in the context of an
         * existing transaction.
         */
        default boolean allowExistingTransaction() {
            return true;
        }
    }

    /**
     * Provides an interface that supports database transactions that do not need to return values.
     */
    @FunctionalInterface
    public interface NonReturningDatabaseTransaction extends DatabaseTransactionFeatures {

        void transaction() throws Exception;
    }

    /** Provides an interface that supports database transactions that need to return values. */
    @FunctionalInterface
    public interface ReturningDatabaseTransaction<T> extends DatabaseTransactionFeatures {

        /**
         * Contains the contents of the transaction. Override in concrete class or lambda.
         *
         * @throws Exception given that the transaction could throw just about anything
         */
        T transaction() throws Exception;
    }
}
