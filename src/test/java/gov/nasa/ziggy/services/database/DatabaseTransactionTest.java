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

/**
 * Unit test class for {@link DatabaseTransaction} and {@link DatabaseTransactionFactory}.
 *
 * @author PT
 */
public class DatabaseTransactionTest {

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
     * Tests DatabaseTransaction functionality in current thread.
     */
    @Test
    public void testTransaction() throws Exception {

        // construct a DatabaseTransaction object
        DatabaseTransaction<Void> dtw = new TestDatabaseTransaction(false, false);
        DatabaseTransaction<Void> spyDtw = spy(dtw);
        DatabaseTransactionFactory.performTransaction(spyDtw);

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
     * Tests DatabaseTransaction functionality in a new thread.
     */
    @Test
    public void testTransactionNewThread() throws Exception {

        // construct a DatabaseTransaction object
        DatabaseTransaction<Void> dtw = new TestDatabaseTransaction(false, false);
        DatabaseTransaction<Void> spyDtw = spy(dtw);
        DatabaseTransactionFactory.performTransactionInThread(spyDtw);

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
     * Tests DatabaseTransaction functionality with a returned value in the calling thread.
     */
    @Test
    public void testTransactionWithReturnValue() throws Exception {

        // construct a DatabaseTransaction object
        DatabaseTransaction<Integer> dtw = new TestDatabaseTransactionWithReturn();
        DatabaseTransaction<Integer> spyDtw = spy(dtw);
        int retval = (Integer) DatabaseTransactionFactory.performTransaction(spyDtw);
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
     * Tests DatabaseTransaction functionality with a returned value in a new thread.
     */
    @Test
    public void testTransactionWithReturnValueNewThread() throws Exception {

        // construct a DatabaseTransaction object
        DatabaseTransaction<Integer> dtw = new TestDatabaseTransactionWithReturn();
        DatabaseTransaction<Integer> spyDtw = spy(dtw);
        int retval = (Integer) DatabaseTransactionFactory.performTransactionInThread(spyDtw);
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
        DatabaseTransaction<Void> dtw = new TestDatabaseTransaction(true, false);
        DatabaseTransaction<Void> spyDtw = spy(dtw);

        // the code should error out
        assertThrows(PipelineException.class, () -> {

            DatabaseTransactionFactory.performTransaction(spyDtw);
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
        DatabaseTransaction<Void> dtw = new TestDatabaseTransaction(true, true);
        DatabaseTransaction<Void> spyDtw = spy(dtw);

        DatabaseTransactionFactory.performTransaction(spyDtw);

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

    private static class TestDatabaseTransaction implements DatabaseTransaction<Void> {

        boolean throwException;
        boolean swallowException;

        public TestDatabaseTransaction(boolean throwException, boolean swallowException) {
            this.throwException = throwException;
            this.swallowException = swallowException;
        }

        @Override
        public Void transaction() throws Exception {
            if (throwException) {
                throw new IOException("Testing of exception handling");
            }
            return null;
        }

        @Override
        public boolean swallowException() {
            return swallowException;
        }
    }

    private static class TestDatabaseTransactionWithReturn implements DatabaseTransaction<Integer> {

        @Override
        public Integer transaction() throws Exception {
            return 5;
        }

    }
}
