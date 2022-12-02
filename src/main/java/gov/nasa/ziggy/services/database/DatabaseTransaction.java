package gov.nasa.ziggy.services.database;

import gov.nasa.ziggy.module.PipelineException;

/**
 * Provides the contents for a database transaction. The abstract method {link
 * {@link #transaction()} contains the contents of the transaction. The transaction itself is
 * executed by the static methods of {@link DatabaseTransactionFactory}; those methods wrap the
 * {@link #transaction()} method in the standard calls to start a transaction, commit the
 * transaction, roll back the transaction in the event of an exception, and close the session once
 * the transaction is completed.
 * <p>
 * The user can also override several default methods in this interface:
 * <ol>
 * <li>{@link #catchBlock(Throwable)} provides additional functionality that is exercised in the
 * catch block of the transaction wrapper; default is no functionality.
 * <li>{@link #finallyBlock()} provides additional functionality that is exercised in the finally
 * block of the transaction wrapper; default is no functionality.
 * <li>{@link #swallowException()} returns a boolean that indicates whether an exception in the try
 * block (i.e., during the transaction} is swallowed (if the method returns true), or if a
 * {@link PipelineException} is thrown at the end of the catch block (if the method returns false).
 * Default is false (i.e., at the end of the catch block throw another exception).
 * </ol>
 * <p>
 *
 * @see gov.nasa.ziggy.services.database.DatabaseTransactionFactory
 * @author PT
 */
@FunctionalInterface
public interface DatabaseTransaction<T> {

    /**
     * Contains the contents of the transaction. Override in concrete class or lambda.
     */
    T transaction() throws Exception;

    /**
     * Provides the capability to execute code in the catch block after the transaction is rolled
     * back. Override to implement such a block.
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
     * Determines whether an exception (specifically, a {@link PipelineException}) is thrown at the
     * end of the catch block. The default behavior is that an exception is thrown, which will
     * generally result in processing coming to a halt. Override in cases in which processing should
     * continue normally despite the failure of the transaction.
     */
    default boolean swallowException() {
        return false;
    }

    /**
     * Determines whether the database transaction is performed without logging messages. The
     * default behavior is to generate logging messages, but in some contexts these are overly
     * distracting or not very useful and so should be suppressed. In those contexts, override this
     * method to return true.
     */
    default boolean silent() {
        return false;
    }

}
