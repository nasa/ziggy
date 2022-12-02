package gov.nasa.ziggy.pipeline.definition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;
import org.mockito.Mockito;

/**
 * Performs unit tests for the {@link PipelineExecutionTime} interface.
 *
 * @author PT
 */
public class PipelineExecutionTimeTest {

    @Test
    public void testStartExecutionClockFirstAttempt() {

        // When the execution clock is started the first time, both start times are set to the
        // current time at that instant.
        PipelineExecutionTime pipelineExecutionTime = new PipelineExecutionTimeImplForTesting();
        Date currentDate = new Date();
        pipelineExecutionTime.startExecutionClock();
        assertTrue(
            pipelineExecutionTime.getStartProcessingTime().getTime() - currentDate.getTime() < 10L);
        assertEquals(pipelineExecutionTime.getStartProcessingTime().getTime(),
            pipelineExecutionTime.getCurrentExecutionStartTimeMillis());
        assertEquals(0L, pipelineExecutionTime.getPriorProcessingExecutionTimeMillis());
        assertEquals(0L, pipelineExecutionTime.getEndProcessingTime().getTime());
    }

    @Test
    public void testStartExecutionClockLaterAttempt() throws ParseException {

        // When the execution time is started for a subsequent attempt at processing (as
        // indicated by the fact that the start time is already set), the current execution
        // start time is set to the current time at that instant but the overall start time
        // is not touched.
        PipelineExecutionTime pipelineExecutionTime = new PipelineExecutionTimeImplForTesting();
        pipelineExecutionTime
            .setStartProcessingTime(new SimpleDateFormat("yyMMddHHmmss").parse("20211229000000"));
        Date currentDate = new Date();
        pipelineExecutionTime.startExecutionClock();
        assertTrue(pipelineExecutionTime.getCurrentExecutionStartTimeMillis()
            - currentDate.getTime() < 10L);
        assertTrue(pipelineExecutionTime.getCurrentExecutionStartTimeMillis()
            - pipelineExecutionTime.getStartProcessingTime().getTime() > 10000000L);
        assertEquals(0L, pipelineExecutionTime.getPriorProcessingExecutionTimeMillis());
        assertEquals(0L, pipelineExecutionTime.getEndProcessingTime().getTime());
    }

    @Test
    public void testStopExecutionClockSetsEndProcessingTime() {

        // When the execution clock is stopped, it should set the end processing time to the
        // current time at that instant.
        PipelineExecutionTime pipelineExecutionTime = new PipelineExecutionTimeImplForTesting();
        Date currentDate = new Date();
        pipelineExecutionTime.stopExecutionClock();
        assertTrue(
            pipelineExecutionTime.getEndProcessingTime().getTime() - currentDate.getTime() < 10L);
    }

    @Test
    public void testStopExecutionClock() throws ParseException {

        // When the stop execution clock is called the first time, the time spent in the first
        // processing attempt is moved into the prior processing time and the start time for
        // the current event is set to -1.
        PipelineExecutionTime pipelineExecutionTime = Mockito
            .spy(PipelineExecutionTimeImplForTesting.class);
        Date startDate = new SimpleDateFormat("yyMMddHHmmss").parse("20211229000000");
        pipelineExecutionTime.setStartProcessingTime(startDate);
        pipelineExecutionTime.setCurrentExecutionStartTimeMillis(
            pipelineExecutionTime.getStartProcessingTime().getTime());
        Mockito.when(pipelineExecutionTime.getEndProcessingTime())
            .thenReturn(new SimpleDateFormat("yyMMddHHmmss").parse("20211229010000"));
        assertEquals(0L, pipelineExecutionTime.getPriorProcessingExecutionTimeMillis());
        pipelineExecutionTime.stopExecutionClock();
        assertEquals(-1L, pipelineExecutionTime.getCurrentExecutionStartTimeMillis());
        assertEquals(60000L, pipelineExecutionTime.getPriorProcessingExecutionTimeMillis());

        // When a second processing attempt is performed,the stopExecutionClock() method
        // increments the prior processing time by the amount of the most recent processing
        // duration.
        pipelineExecutionTime.setCurrentExecutionStartTimeMillis(startDate.getTime());
        pipelineExecutionTime.stopExecutionClock();
        assertEquals(120000L, pipelineExecutionTime.getPriorProcessingExecutionTimeMillis());
        assertEquals(-1L, pipelineExecutionTime.getCurrentExecutionStartTimeMillis());
    }

    @Test
    public void testTotalTimeAllAttemptsMillis() throws ParseException {

        // When the total time from all processing attempts is requested and we're on the
        // first processing attempt, the total is just the time from the start of current
        // attempt to the current instant.
        PipelineExecutionTime pipelineExecutionTime = Mockito
            .spy(PipelineExecutionTimeImplForTesting.class);
        Date startDate = new SimpleDateFormat("yyMMddHHmmss").parse("20211229000000");
        pipelineExecutionTime.setStartProcessingTime(startDate);
        pipelineExecutionTime.setCurrentExecutionStartTimeMillis(
            pipelineExecutionTime.getStartProcessingTime().getTime());
        Mockito.when(pipelineExecutionTime.currentTimeMillis())
            .thenReturn(new SimpleDateFormat("yyMMddHHmmss").parse("20211229010000").getTime());
        long exeTime = pipelineExecutionTime.totalExecutionTimeAllAttemptsMillis();
        assertEquals(60000L, exeTime);

        // On subsequent attempts, the value from prior processing attempts is added to the
        // current processing attempt.
        pipelineExecutionTime.setPriorProcessingExecutionTimeMillis(100L);
        exeTime = pipelineExecutionTime.totalExecutionTimeAllAttemptsMillis();
        assertEquals(60100L, exeTime);
    }

    public static class PipelineExecutionTimeImplForTesting implements PipelineExecutionTime {

        private Date startProcessingTime = new Date(0);
        private Date endProcessingTime = new Date(0);
        private long totalExecutionTimeMillis;
        private long currentExecutionStartTimeMillis;

        public PipelineExecutionTimeImplForTesting() {
            currentExecutionStartTimeMillis = -1;
        }

        @Override
        public void setStartProcessingTime(Date date) {
            startProcessingTime = date;
        }

        @Override
        public Date getStartProcessingTime() {
            return startProcessingTime;
        }

        @Override
        public void setEndProcessingTime(Date date) {
            endProcessingTime = date;
        }

        @Override
        public Date getEndProcessingTime() {
            return endProcessingTime;
        }

        @Override
        public void setPriorProcessingExecutionTimeMillis(long executionTimeMillis) {
            totalExecutionTimeMillis = executionTimeMillis;
        }

        @Override
        public long getPriorProcessingExecutionTimeMillis() {
            return totalExecutionTimeMillis;
        }

        @Override
        public void setCurrentExecutionStartTimeMillis(long linuxStartTimeCurrentExecution) {
            currentExecutionStartTimeMillis = linuxStartTimeCurrentExecution;
        }

        @Override
        public long getCurrentExecutionStartTimeMillis() {
            return currentExecutionStartTimeMillis;
        }
    }
}
