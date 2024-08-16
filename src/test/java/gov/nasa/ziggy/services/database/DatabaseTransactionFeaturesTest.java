package gov.nasa.ziggy.services.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.database.DatabaseOperations.NonReturningDatabaseTransaction;
import gov.nasa.ziggy.services.database.DatabaseOperations.ReturningDatabaseTransaction;

/**
 * Unit test class for {@link DatabaseTransactionFeatures}.
 *
 * @author PT
 */
public class DatabaseTransactionFeaturesTest {

    private DatabaseService dbService = mock(DatabaseService.class);

    @Before
    public void setup() {
        DatabaseService.setInstance(dbService);
    }

    @After
    public void teardown() {
        DatabaseService.reset();
    }

    /**
     * Tests DatabaseTransaction functionality.
     */
    @Test
    public void testTransaction() throws Exception {

        // construct a DatabaseTransaction object
//        NonReturningDatabaseTransaction spyDtw = spy(new TestDatabaseTransaction(false, false));
        NonReturningDatabaseTransaction spyDtw = new TestOperations().testDatabaseTransaction(false,
            false);
//        performTransaction(spyDtw);

        // verify function calls
        verify(dbService).beginTransaction();
        verify(spyDtw).transaction();
        verify(dbService).commitTransaction();
        verify(dbService).closeCurrentSession();
        verify(spyDtw).finallyBlock();

        // verify the calls that we DO NOT want to see in this use case
        verify(dbService, times(0)).rollbackTransactionIfActive();
        verify(spyDtw, times(0)).catchBlock(any(Throwable.class));
    }

    /**
     * Tests DatabaseTransaction functionality with a returned value.
     */
    @Test
    public void testTransactionWithReturnValue() throws Exception {

        // construct a DatabaseTransaction object
        ReturningDatabaseTransaction<Integer> spyDtw = spy(new TestDatabaseTransactionWithReturn());
        int retval = new TestOperations().testDatabaseTransactionWithReturn(spyDtw);
        assertEquals(5, retval);

        // verify function calls
        verify(dbService).beginTransaction();
        verify(spyDtw).transaction();
        verify(dbService).commitTransaction();
        verify(dbService).closeCurrentSession();
        verify(spyDtw).finallyBlock();

        // verify the calls that we DO NOT want to see in this use case
        verify(dbService, times(0)).rollbackTransactionIfActive();
        verify(spyDtw, times(0)).catchBlock(any(Throwable.class));
    }

    /**
     * Tests execution flow in the case in which there is an exception at some point in the try
     * block.
     */
    @Test
    public void testErrorExecution() throws Exception {

        // construct a DatabaseTransaction object
        NonReturningDatabaseTransaction spyDtw = spy(new TestDatabaseTransaction(true, false));

        // the code should error out
        assertThrows(PipelineException.class, () -> {
            new TestOperations().testDatabaseTransaction(spyDtw);
        });

        // verify function calls
        verify(dbService).beginTransaction();
        verify(spyDtw).transaction();
        verify(dbService).rollbackTransactionIfActive();
        verify(dbService).closeCurrentSession();
        verify(spyDtw).catchBlock(any(Throwable.class));
        verify(spyDtw).finallyBlock();

        // verify the things we don't want to see called
        verify(dbService, times(0)).commitTransaction();
    }

    @Test
    public void testErrorExecutionWithoutPipelineException() throws Exception {

        // construct a DatabaseTransaction object
        NonReturningDatabaseTransaction spyDtw = new TestOperations().testDatabaseTransaction(true,
            true);

        // verify function calls
        verify(dbService).beginTransaction();
        verify(spyDtw).transaction();
        verify(dbService).rollbackTransactionIfActive();
        verify(dbService).closeCurrentSession();
        verify(spyDtw).catchBlock(any(Throwable.class));
        verify(spyDtw).finallyBlock();

        // verify the things we don't want to see called
        verify(dbService, times(0)).commitTransaction();
    }

    private static class TestDatabaseTransaction implements NonReturningDatabaseTransaction {

        boolean throwException;
        boolean swallowException;

        public TestDatabaseTransaction(boolean throwException, boolean swallowException) {
            this.throwException = throwException;
            this.swallowException = swallowException;
        }

        @Override
        public void transaction() throws Exception {
            if (throwException) {
                throw new IOException("Testing of exception handling");
            }
        }

        @Override
        public boolean swallowException() {
            return swallowException;
        }
    }

    private static class TestDatabaseTransactionWithReturn
        implements ReturningDatabaseTransaction<Integer> {

        @Override
        public Integer transaction() throws Exception {
            return 5;
        }
    }

    private static class TestOperations extends DatabaseOperations {

        public TestDatabaseTransaction testDatabaseTransaction(boolean throwException,
            boolean swallowException) {
            TestDatabaseTransaction testDatabaseTransaction = spy(
                new TestDatabaseTransaction(throwException, swallowException));
            performTransaction(testDatabaseTransaction);
            return testDatabaseTransaction;
        }

        public void testDatabaseTransaction(NonReturningDatabaseTransaction transaction) {
            performTransaction(transaction);
        }

        public int testDatabaseTransactionWithReturn(
            ReturningDatabaseTransaction<Integer> transaction) {
            return performTransaction(transaction);
        }
    }
}
