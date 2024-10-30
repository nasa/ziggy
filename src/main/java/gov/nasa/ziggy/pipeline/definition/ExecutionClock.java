package gov.nasa.ziggy.pipeline.definition;

import java.util.Objects;

import org.apache.commons.lang3.time.DurationFormatUtils;

import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.util.SystemProxy;
import jakarta.persistence.Embeddable;

/**
 * Provides total execution time for {@link PipelineTask} and {@link PipelineInstance} objects. All
 * times are expressed in milliseconds since the start of the Unix epoch, with the exception of
 * {@link #toString()}, which returns a {@link String} of the total execution time in "HH:mm:ss"
 * format.
 *
 * @author PT
 * @author Bill Wohler
 */
@Embeddable
public class ExecutionClock {

    /** Timestamp that processing most recently started. */
    private long startProcessingTime;

    /** Total time spent processing prior execution attempts. */
    private long priorProcessingExecutionTime;

    private boolean running;

    /**
     * Starts execution timing for an instance. At start time, the processing-start time is set.
     * <p>
     * Users are advised not to call this method directly. The {@link PipelineTaskOperations} class
     * will call this method as needed.
     */
    public void start() {

        // Only start the clock if it's not currently running.
        if (running) {
            return;
        }

        startProcessingTime = SystemProxy.currentTimeMillis();
        running = true;
    }

    /**
     * Stops execution timing for an instance. The current-execution start time is set to -1 and the
     * elapsed time from the current execution attempt is added to the total execution time. The end
     * processing time is also set.
     * <p>
     * Users are advised not to call this method directly. The {@link PipelineTaskOperations} class
     * will call this method as needed.
     */
    public void stop() {

        // Only stop the clock if it's currently running.
        if (!running) {
            return;
        }

        priorProcessingExecutionTime += SystemProxy.currentTimeMillis() - startProcessingTime;
        running = false;
    }

    /**
     * Calculates the total execution time for all processing attempts to date, in milliseconds. The
     * total time includes the sum of the total execution time for all prior efforts; if a
     * processing effort is current underway, the duration of the current effort is added to the
     * total execution time from all prior efforts.
     */
    public long totalExecutionTime() {
        long totalExecutionTime = priorProcessingExecutionTime;
        if (running) {
            totalExecutionTime += SystemProxy.currentTimeMillis() - startProcessingTime;
        }
        return totalExecutionTime;
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    public int hashCode() {
        return Objects.hash(priorProcessingExecutionTime, running, startProcessingTime);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ExecutionClock other = (ExecutionClock) obj;
        return priorProcessingExecutionTime == other.priorProcessingExecutionTime
            && running == other.running && startProcessingTime == other.startProcessingTime;
    }

    /**
     * Returns the elapsed time for processing (both current attempt and any prior ones) in
     * "HH:mm:ss" format. If no processing has occurred, "-" is returned. This is the same time
     * value that is returned by {@link #totalExecutionTime()}, but formatted for display
     * convenience.
     */
    @Override
    public String toString() {
        if (startProcessingTime == 0) {
            return "-";
        }
        return DurationFormatUtils.formatDuration(totalExecutionTime(), "HH:mm:ss");
    }
}
