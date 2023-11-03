package gov.nasa.ziggy.pipeline.definition;

import java.util.Date;

import org.apache.commons.lang3.time.DurationFormatUtils;

import gov.nasa.ziggy.pipeline.PipelineOperations;

/**
 * Provides capabilities needed by classes that track pipeline execution times, such as
 * {@link PipelineTask} and {@link PipelineInstance}. Four pieces of information are managed:
 * <ol>
 * <li>The start time of the very first processing attempt (a {@link Date})
 * <li>The start time of the current processing attempt (in milliseconds since the start of Linux
 * time)
 * <li>the total elapsed execution time for all prior processing attempts, in milliseconds
 * <li>The end time of the most recent processing attempt (a {@link Date}).
 * </ol>
 * <p>
 * In addition to getters and setters for the above values, classes that implement
 * {@link PipelineExecutionTime} are provided with additional, higher-level methods that start and
 * stop the execution "clock," and which calculate the current total elapsed processing time.
 *
 * @author PT
 */
public interface PipelineExecutionTime {

    void setStartProcessingTime(Date date);

    Date getStartProcessingTime();

    void setEndProcessingTime(Date date);

    Date getEndProcessingTime();

    void setPriorProcessingExecutionTimeMillis(long executionTimeMillis);

    long getPriorProcessingExecutionTimeMillis();

    void setCurrentExecutionStartTimeMillis(long linuxStartTimeCurrentExecution);

    long getCurrentExecutionStartTimeMillis();

    /**
     * Starts execution timing for an instance. At start time, the processing-start time is set if
     * currently null, and the start time for the current execution time is also set.
     * <p>
     * Users are advised not to call this method directly. The {@link PipelineOperations} class will
     * call this method when a change in state of a {@link PipelineExecutionTime} implementation
     * requires it.
     */
    default void startExecutionClock() {

        // Only start the clock if it's not currently running; this is indicated by the
        // current execution start time being an invalid value.
        if (getCurrentExecutionStartTimeMillis() <= 0) {
            if (getStartProcessingTime() == null || getStartProcessingTime().getTime() <= 0) {
                setStartProcessingTime(new Date());
            }
            setCurrentExecutionStartTimeMillis(currentTimeMillis());
        }
    }

    /**
     * Stops execution timing for an instance. The current-execution start time is set to -1 and the
     * elapsed time from the current execution attempt is added to the total execution time. The end
     * processing time is also set.
     * <p>
     * Users are advised not to call this method directly. The {@link PipelineOperations} class will
     * call this method when a change in state of a {@link PipelineExecutionTime} implementation
     * requires it.
     */
    default void stopExecutionClock() {

        // Only stop the clock if it's currently running; this is indicated by the
        // current execution start time being valid.
        if (getCurrentExecutionStartTimeMillis() > 0) {
            setEndProcessingTime(new Date());
            setPriorProcessingExecutionTimeMillis(getPriorProcessingExecutionTimeMillis()
                + getEndProcessingTime().getTime() - getCurrentExecutionStartTimeMillis());
            setCurrentExecutionStartTimeMillis(-1);
        }
    }

    /**
     * Calculates the total execution time for all processing attempts to date, in milliseconds. The
     * total time includes the sum of the total execution time for all prior efforts; if a
     * processing effort is current underway, the duration of the current effort is added to the
     * total execution time from all prior efforts.
     */
    default long totalExecutionTimeAllAttemptsMillis() {
        long totalExecutionTime = getPriorProcessingExecutionTimeMillis();
        long startTimeCurrentExecutionMillis = getCurrentExecutionStartTimeMillis();
        if (startTimeCurrentExecutionMillis >= 0) {
            totalExecutionTime += currentTimeMillis() - startTimeCurrentExecutionMillis;
        }
        return totalExecutionTime;
    }

    // The current time in milliseconds since the start of the Unix era is obtained
    // via a method for testing purposes.
    default long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    default String elapsedTime() {

        if (getStartProcessingTime().getTime() == 0) {
            return "-";
        }
        return DurationFormatUtils.formatDuration(totalExecutionTimeAllAttemptsMillis(),
            "HH:mm:ss");
    }
}
