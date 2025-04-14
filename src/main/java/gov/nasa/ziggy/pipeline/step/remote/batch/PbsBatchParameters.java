package gov.nasa.ziggy.pipeline.step.remote.batch;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineNodeExecutionResources;
import gov.nasa.ziggy.pipeline.step.remote.Architecture;
import gov.nasa.ziggy.pipeline.step.remote.BatchParameters;
import gov.nasa.ziggy.pipeline.step.remote.BatchQueue;
import gov.nasa.ziggy.pipeline.step.remote.RemoteArchitectureOptimizer;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.util.RealValueFormatter;
import gov.nasa.ziggy.util.TimeFormatter;

/** {@link BatchParameters} class for the Portable Batch System (PBS). */
public class PbsBatchParameters implements BatchParameters {

    private static final Logger log = LoggerFactory.getLogger(PbsBatchParameters.class);

    private static final double WALL_TIME_ROUNDING_INTERVAL_HOURS = 0.25;

    private PipelineNodeExecutionResources executionResources;

    private String requestedWallTime;
    private int requestedNodeCount;
    private BatchQueue batchQueue;
    private String remoteGroup;
    private Architecture architecture;
    private int activeCoresPerNode;
    private double estimatedCost;
    boolean valuesComputed;

    @Override
    public void computeParameterValues(PipelineNodeExecutionResources executionResources,
        int totalSubtasks) {
        this.executionResources = executionResources;
        architecture = executionResources.getArchitecture();
        batchQueue = executionResources.getBatchQueue();

        populateArchitecture(totalSubtasks);
        populateResourceParameters(totalSubtasks);
        valuesComputed = true;
    }

    /**
     * Selects an architecture based on optimization, or the user's specification of same.
     *
     * @param totalSubtasks total number of subtasks
     */
    private void populateArchitecture(int totalSubtasks) {

        // If the architecture isn't specified, determine it via optimization.
        if (architecture == null) {
            selectArchitecture(totalSubtasks);
        } else {
            // Note that if the architecture IS specified, it may still not be compatible
            // with the RAM requirements; check that possibility now.
            validateArchitecture();
        }
    }

    /**
     * Selects an architecture based on optimization.
     *
     * @param totalSubtasks total number of subtasks
     */
    private void selectArchitecture(int totalSubtasks) {
        RemoteArchitectureOptimizer optimizer = optimizer();
        architecture = optimizer.optimalNodeArchitecture(this, totalSubtasks);
    }

    private void validateArchitecture() {
        if (!architecture.hasSufficientRam(executionResources.getGigsPerSubtask())) {
            throw new IllegalStateException("Selected node architecture " + architecture.toString()
                + " has insufficient RAM for subtasks that require "
                + executionResources.getGigsPerSubtask() + " GB");
        }
    }

    /**
     * Selects an optimizer. Usually this is the optimizer set in the execution resources, unless
     * that optimizer is invalid given the rest of the configuration, in which case optimization is
     * on cost.
     */
    private RemoteArchitectureOptimizer optimizer() {
        RemoteArchitectureOptimizer optimizer = executionResources.getOptimizer();

        // If node sharing is turned off, the cores optimization doesn't make sense.
        if (optimizer.equals(RemoteArchitectureOptimizer.CORES)
            && !executionResources.isNodeSharing()) {
            log.warn("CORES optimization invalid in absence of node sharing, using COST instead");
            return RemoteArchitectureOptimizer.COST;
        }

        // If the environment doesn't have queue time metrics, we can't optimize on them.
        if (optimizer.equals(RemoteArchitectureOptimizer.QUEUE_DEPTH)
            || optimizer.equals(RemoteArchitectureOptimizer.QUEUE_TIME)
                && executionResources.getRemoteEnvironment().getQueueTimeMetricsClass() == null) {
            log.warn("Optimizer {} invalid for environment {}, using COST instead",
                optimizer.toString(), executionResources.getRemoteEnvironment().getName());
            return RemoteArchitectureOptimizer.COST;
        }
        return optimizer;
    }

    /**
     * Populates the resource-related parameters: specifically, the number of nodes, the number of
     * subtasks per node, the wall time request, the queue, and the remote group to be billed for
     * the PBS jobs. An estimate of the cost is also performed.
     */
    private void populateResourceParameters(int totalSubtasks) {
        activeCoresPerNode = activeCoresPerNode(totalSubtasks);
        double subtasksPerCore = subtasksPerCore(totalSubtasks);

        // Set the number of nodes and the number of subtasks per node.
        requestedNodeCount = computeNodeRequest(totalSubtasks, subtasksPerCore);

        // Set the wall time and the queue.
        requestedWallTime = computeWallTimeAndQueue(subtasksPerCore);
        if (executionResources.getBatchQueue() == null) {
            batchQueue = selectQueue(subtasksPerCore);
        } else {
            batchQueue = executionResources.getBatchQueue();
            validateQueue();
        }

        // Estimate the costs.
        estimatedCost = computeEstimatedCost();

        // Set the remote group.
        remoteGroup = ZiggyConfiguration.getInstance()
            .getString(
                PropertyName.remoteGroup(executionResources.getRemoteEnvironment().getName()), "");
    }

    /**
     * Computes the active cores per node. This is determined by the execution mode (sharing or not
     * sharing nodes between multiple subtasks) and the RAM per compute node (which is only
     * sufficient to support a limited number of subtasks in parallel).
     */
    private int activeCoresPerNode(int totalSubtasks) {
        if (!executionResources.isNodeSharing()) {
            return 1;
        }
        double gigsPerNode = architecture.getRamGigabytes();
        return Math
            .min((int) Math.min(Math.floor(gigsPerNode / executionResources.getGigsPerSubtask()),
                architecture.getCores()), totalSubtasks);
    }

    /**
     * Number of subtasks each active core will need to process (effectively, the number of "waves"
     * of subtasks that the nodes will have to process).
     */
    private double subtasksPerCore(int totalSubtasks) {
        double wallTimeRatio = executionResources.getSubtaskMaxWallTimeHours()
            / executionResources.getSubtaskTypicalWallTimeHours();
        double subtasksPerCore = wallTimeRatio;

        // If the number of nodes is limited, compute how many subtasks per active core
        // are needed; if this number is larger, keep it
        if (executionResources.getMaxNodes() > 0) {
            subtasksPerCore = Math.max(subtasksPerCore,
                (double) totalSubtasks / (activeCoresPerNode * executionResources.getMaxNodes()));
        }

        // Finally, if the user has supplied an override for subtasks per core, apply it
        // unless it conflicts with the value needed to make the job complete
        if (executionResources.getSubtasksPerCore() > 0) {
            double overrideSubtasksPerCore = executionResources.getSubtasksPerCore();
            if (overrideSubtasksPerCore >= subtasksPerCore) {
                subtasksPerCore = overrideSubtasksPerCore;
            } else {
                log.warn(
                    "User-supplied subtasks per core {} too small, using algorithmic value of {}",
                    overrideSubtasksPerCore, subtasksPerCore);
            }
        }
        return subtasksPerCore;
    }

    /** Determines the number of nodes to be requested from the batch system. */
    private int computeNodeRequest(int totalSubtasks, double subtasksPerCore) {

        // Compute the number needed to process all subtasks in a single "wave."
        if (executionResources.getMaxNodes() <= 0) {
            return computeRequestedNodeCount(totalSubtasks, subtasksPerCore);
        }

        // Apply any constraints from the user.
        return Math.min(executionResources.getMaxNodes(),
            computeRequestedNodeCount(totalSubtasks, subtasksPerCore));
    }

    /** Determines the number of nodes to request in order to run all subtasks in parallel. */
    private int computeRequestedNodeCount(int totalSubtasks, double subtasksPerCore) {

        double nodesNeeded = totalSubtasks / (subtasksPerCore * activeCoresPerNode);
        return (int) Math.ceil(Math.max(1, nodesNeeded));
    }

    /**
     * Determines the queue and wall time request for the job.
     * <p>
     * The wall time is given by the number of nodes, the number of subtasks, and the approximate
     * execution time per subtask; the wall time must be sufficient to allow the job to complete
     * with these constraints. The wall time is rounded up to the nearest quarter hour.
     * <p>
     * The queue is the queue that permits sufficient wall time to complete execution. If multiple
     * queues meet this requirement, the one with the lowest value for its maximum wall time is
     * selected.
     * <p>
     * If the user has selected a reserved queue, the {@link BatchQueue} stored in this object will
     * be a reserved queue with the reserved queue name set.
     */
    private String computeWallTimeAndQueue(double subtasksPerCore) {

        double unpaddedWallTimeHours = unpaddedWallTime(executionResources, subtasksPerCore);
        double requestedWallTimeHours = roundToNearest(unpaddedWallTimeHours,
            WALL_TIME_ROUNDING_INTERVAL_HOURS);
        return TimeFormatter.timeInHoursToStringHhMmSs(requestedWallTimeHours);
    }

    /** Selects a queue for the job based on the maximum wall time allowed for each queue. */
    private BatchQueue selectQueue(double subtasksPerCore) {
        double requestedWallTimeHours = TimeFormatter
            .timeStringHhMmSsToTimeInHours(requestedWallTime);
        List<BatchQueue> queues = BatchQueue
            .autoSelectableBatchQueues(executionResources.getRemoteEnvironment().getQueues());
        for (BatchQueue queue : queues) {
            if (queue.getMaxWallTimeHours() >= requestedWallTimeHours) {
                return queue;
            }
        }
        throw new IllegalStateException("No queues can support requested wall time of "
            + TimeFormatter.stripSeconds(requestedWallTime));
    }

    /**
     * Validates that a selected queue permits sufficient wall time. If the selected queue is
     * reserved, obtain the name of the reserved queue.
     */
    private void validateQueue() {
        double requestedWallTimeHours = TimeFormatter
            .timeStringHhMmSsToTimeInHours(requestedWallTime);
        if (requestedWallTimeHours > batchQueue.getMaxWallTimeHours()) {
            throw new IllegalStateException(
                "Queue " + batchQueue.getName() + " cannot support job with wall time of "
                    + TimeFormatter.stripSeconds(requestedWallTime));
        }
        if (batchQueue.isReserved()) {
            batchQueue = BatchQueue.reservedBatchQueueWithQueueName(batchQueue,
                executionResources.getReservedQueueName());
        }
    }

    /** Calculates the wall time required for the current task before any padding is added. */
    private double unpaddedWallTime(PipelineNodeExecutionResources executionResources,
        double subtasksPerCore) {

        double subtaskMaxWallTimeHours = executionResources.getSubtaskMaxWallTimeHours();
        double wallTimeFromTypical = subtasksPerCore
            * executionResources.getSubtaskTypicalWallTimeHours();

        if (!executionResources.isNodeSharing() && executionResources.isWallTimeScaling()) {
            // If we're using internal parallelization, we can scale down the wall time by the
            // cores per node.
            subtaskMaxWallTimeHours /= architecture.getCores();
            wallTimeFromTypical /= architecture.getCores();
        } else {
            // If we're not using internal parallelization, round the times to the nearest
            // multiple of the typical wall time.
            subtaskMaxWallTimeHours = roundToNearest(subtaskMaxWallTimeHours,
                executionResources.getSubtaskTypicalWallTimeHours());
            wallTimeFromTypical = roundToNearest(wallTimeFromTypical,
                executionResources.getSubtaskTypicalWallTimeHours());
        }
        return Math.max(wallTimeFromTypical, subtaskMaxWallTimeHours);
    }

    private double roundToNearest(double value, double roundingInterval) {
        return roundingInterval * Math.ceil(value / roundingInterval);
    }

    /**
     * Computes the estimated cost, which is just the cost per node, multipled by number of nodes,
     * multiplied by hours of wall time requested.
     */
    private double computeEstimatedCost() {
        return architecture.getCost() * requestedNodeCount
            * TimeFormatter.timeStringHhMmSsToTimeInHours(requestedWallTime);
    }

    @Override
    public String batchParameterSetName() {
        return "PBS Parameters";
    }

    @Override
    public Map<String, String> batchParametersByName(String costUnit) {
        Map<String, String> batchParametersByName = new LinkedHashMap<>();
        batchParametersByName.put("Architecture:",
            valuesComputed ? architecture.getDescription() : "");
        batchParametersByName.put("Queue:", valuesComputed ? batchQueue.getDescription() : "");
        batchParametersByName.put("Wall time:", valuesComputed ? requestedWallTime : "");
        batchParametersByName.put("Node count:",
            valuesComputed ? Integer.toString(requestedNodeCount) : "");
        batchParametersByName.put("Active cores per node:",
            valuesComputed ? Integer.toString(activeCoresPerNode) : "");
        costUnit = !StringUtils.isBlank(costUnit) ? costUnit : "???";
        batchParametersByName.put("Estimated cost:",
            valuesComputed ? RealValueFormatter.costFormatter(estimatedCost) + " " + costUnit : "");

        return batchParametersByName;
    }

    @Override
    public PipelineNodeExecutionResources executionResources() {
        return executionResources;
    }

    @Override
    public double requestedWallTimeHours() {

        return !StringUtils.isBlank(requestedWallTime)
            ? TimeFormatter.timeStringHhMmSsToTimeInHours(requestedWallTime)
            : -1;
    }

    @Override
    public double estimatedCost() {
        return estimatedCost;
    }

    @Override
    public int nodeCount() {
        return requestedNodeCount;
    }

    @Override
    public String displayMessage() {
        if (!valuesComputed) {
            return null;
        }
        float maxWallTimeHours = batchQueue.getMaxWallTimeHours();
        return MessageFormat.format(
            "The {0} architecture has {1} cores, {2} GB/core, and a cost factor of {3}, "
                + "and for the {4} queue, the limit is {5} hrs max wall time.",
            architecture.getDescription(), architecture.getCores(), architecture.gigsPerCore(),
            architecture.getCost(), batchQueue.getDescription(),
            maxWallTimeHours == Float.MAX_VALUE ? "infinite" : maxWallTimeHours);
    }

    /**
     * Performs aggregation of {@link PbsBatchParameters} instances. Implemented in this class to
     * allow access to private fields.
     */
    static PbsBatchParameters aggregate(Collection<BatchParameters> parametersInstances) {
        PbsBatchParameters aggregatedParameters = new PbsBatchParameters();

        // See whether parameters have been computed.
        aggregatedParameters.valuesComputed = true;
        for (BatchParameters batchParameters : parametersInstances) {
            PbsBatchParameters pbsBatchParameters = (PbsBatchParameters) batchParameters;
            if (!pbsBatchParameters.valuesComputed) {
                aggregatedParameters.valuesComputed = false;
                break;
            }
        }

        // Loop over PbsParameters instances and collect values.
        for (BatchParameters batchParameters : parametersInstances) {

            PbsBatchParameters pbsBatchParameters = (PbsBatchParameters) batchParameters;
            // Start with the fields that are the same for all members of the collection.
            aggregatedParameters.batchQueue = pbsBatchParameters.batchQueue;
            aggregatedParameters.architecture = pbsBatchParameters.architecture;
            aggregatedParameters.remoteGroup = pbsBatchParameters.remoteGroup;

            // Next the fields where we need to find the maximum value across all
            // members of the collection.
            double wallTimeHours = StringUtils.isBlank(aggregatedParameters.getRequestedWallTime())
                ? 0
                : TimeFormatter
                    .timeStringHhMmSsToTimeInHours(aggregatedParameters.getRequestedWallTime());
            wallTimeHours = Math.max(wallTimeHours, TimeFormatter
                .timeStringHhMmSsToTimeInHours(pbsBatchParameters.getRequestedWallTime()));
            aggregatedParameters.requestedWallTime = TimeFormatter
                .timeInHoursToStringHhMmSs(wallTimeHours);
            aggregatedParameters.activeCoresPerNode = Math.max(aggregatedParameters.activeCores(),
                pbsBatchParameters.activeCores());
            BatchQueue batchQueue = pbsBatchParameters.batchQueue;
            if (aggregatedParameters.batchQueue == null) {
                aggregatedParameters.batchQueue = batchQueue;
            }
            aggregatedParameters.batchQueue = aggregatedParameters.batchQueue
                .getMaxWallTimeHours() > pbsBatchParameters.batchQueue.getMaxWallTimeHours()
                    ? aggregatedParameters.batchQueue
                    : pbsBatchParameters.batchQueue;

            // Finally the fields where we need to sum across the collection.
            aggregatedParameters.requestedNodeCount += pbsBatchParameters.nodeCount();
            aggregatedParameters.estimatedCost += pbsBatchParameters.estimatedCost;
        }
        return aggregatedParameters;
    }

    public String getRequestedWallTime() {
        return requestedWallTime;
    }

    public BatchQueue getBatchQueue() {
        return batchQueue;
    }

    public String getRemoteGroup() {
        return remoteGroup;
    }

    public Architecture getArchitecture() {
        return architecture;
    }

    @Override
    public int activeCores() {
        return activeCoresPerNode;
    }
}
