package gov.nasa.ziggy.pipeline.step.remote;

import java.util.List;

/** Provides metrics related to queue times. */
public interface QueueTimeMetrics {

    /**
     * Sets the collection of {@link Architecture}s that the queue time metrics implementation will
     * need to support (by which we mean, "The architectures we need to find queue time metrics
     * for").
     */
    void setArchitectures(List<Architecture> architectures);

    /**
     * Calculates the "runout," which is the time it would take to run all the jobs scheduled for a
     * given architecture.
     */
    double queueDepthHours(Architecture architecture);

    /**
     * Calculates the "expansion" metric: the typical multiplicative factor between the requested
     * wall time and the time including time in queue, for a given architecture.
     */
    double queueTimeFactor(Architecture architecture);
}
