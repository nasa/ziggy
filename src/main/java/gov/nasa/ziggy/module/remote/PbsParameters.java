package gov.nasa.ziggy.module.remote;

import static gov.nasa.ziggy.module.remote.RemoteNodeDescriptor.descriptorsSortedByCores;
import static gov.nasa.ziggy.module.remote.RemoteNodeDescriptor.descriptorsSortedByCost;
import static gov.nasa.ziggy.module.remote.RemoteNodeDescriptor.descriptorsSortedByRamThenCost;
import static gov.nasa.ziggy.module.remote.RemoteNodeDescriptor.nodeHasSufficientRam;
import static gov.nasa.ziggy.module.remote.RemoteNodeDescriptor.nodesWithSufficientRam;
import static gov.nasa.ziggy.module.remote.RemoteQueueDescriptor.descriptorsSortedByMaxTime;

import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNodeExecutionResources;
import gov.nasa.ziggy.util.TimeFormatter;

/**
 * Parameters needed to submit a job via the PBS batch system. Parameters are derived from an
 * appropriate {@link PipelineDefinitionNodeExecutionResources} instance. Any optional parameters
 * will be determined by the needs of the job as determined from the required parameters.
 *
 * @author PT
 */
public class PbsParameters {

    private static final Logger log = LoggerFactory.getLogger(PbsParameters.class);

    private static final double WALL_TIME_ROUNDING_INTERVAL_HOURS = 0.25;

    private boolean enabled;
    private String requestedWallTime;
    private int requestedNodeCount;
    private String queueName;
    private double minGigsPerNode;
    private int minCoresPerNode;
    private double gigsPerSubtask;
    private String remoteGroup;
    private RemoteNodeDescriptor architecture;
    private int activeCoresPerNode;
    private double estimatedCost;

    /**
     * Populates the architecture property of the {@link PbsParameters} instance. If the
     * architecture is not specified already, it is selected by use of the architecture optimization
     * option in {@link PipelineDefinitionNodeExecutionResources}. If it is specified, the
     * architecture is nonetheless checked to ensure that there is sufficient RAM on the selected
     * architecture to run at least 1 subtask per node.
     */
    public void populateArchitecture(PipelineDefinitionNodeExecutionResources executionResources,
        int totalSubtasks, SupportedRemoteClusters remoteCluster) {

        // If the architecture isn't specified, determine it via optimization.
        if (getArchitecture() == null) {
            log.info("Selecting infrastructure for {} subtasks using optimizer {}", totalSubtasks,
                executionResources.getOptimizer());
            selectArchitecture(executionResources, totalSubtasks, remoteCluster);
        }

        // Note that if the architecture IS specified, it may still not be compatible
        // with the RAM requirements; check that possibility now.
        if (executionResources.isNodeSharing()
            && !nodeHasSufficientRam(getArchitecture(), executionResources.getGigsPerSubtask())) {
            throw new IllegalStateException("Selected node architecture "
                + getArchitecture().toString() + " has insufficient RAM for subtasks that require "
                + executionResources.getGigsPerSubtask() + " GB");
        }
    }

    /**
     * Selects an architecture using an optimizer option. The architecture will be one of the ones
     * that corresponds to the {@link SupportedRemoteCluster}, and will be selected from the ones
     * with sufficient RAM to perform the processing.
     */
    private void selectArchitecture(PipelineDefinitionNodeExecutionResources executionResources,
        int totalSubtasks, SupportedRemoteClusters remoteCluster) {

        double gigsPerSubtask = executionResources.getGigsPerSubtask();
        RemoteArchitectureOptimizer optimizer = executionResources.getOptimizer();
        List<RemoteNodeDescriptor> acceptableNodes = null;
        if (!executionResources.isNodeSharing()) {
            acceptableNodes = descriptorsSortedByCores(remoteCluster);
        } else {
            if (optimizer.equals(RemoteArchitectureOptimizer.COST)) {
                acceptableNodes = nodesWithSufficientRam(descriptorsSortedByCost(remoteCluster),
                    gigsPerSubtask);
            } else {
                acceptableNodes = nodesWithSufficientRam(
                    descriptorsSortedByRamThenCost(remoteCluster), gigsPerSubtask);
            }
            if (acceptableNodes.isEmpty()) {
                throw new PipelineException(
                    "All remote architectures have insufficient RAM for subtasks that require "
                        + gigsPerSubtask + " GB");
            }
        }

        architecture = optimizer.optimalArchitecture(executionResources, totalSubtasks,
            acceptableNodes);
    }

    /**
     * Populates the resource-related parameters: specifically, the number of nodes, the number of
     * subtasks per node, the wall time request, the queue, and the remote group to be billed for
     * the PBS jobs. An estimate of the cost (in SBUs or dollars) is also performed.
     */
    public void populateResourceParameters(
        PipelineDefinitionNodeExecutionResources executionResources, int totalSubtaskCount) {

        computeActiveCoresPerNode(executionResources, totalSubtaskCount);
        double subtasksPerCore = subtasksPerCore(executionResources, totalSubtaskCount);

        // Set the number of nodes and the number of subtasks per node
        computeNodeRequest(executionResources, totalSubtaskCount, subtasksPerCore);

        // Set the wall time and the queue
        computeWallTimeAndQueue(executionResources, subtasksPerCore);

        // Estimate the costs
        computeEstimatedCost();

        // Set the remote group
        remoteGroup = RemoteExecutionProperties.getGroup();
    }

    /**
     * Fills in parameters related to the number of nodes to be requested and the number of subtasks
     * that can run in parallel on each node. This takes into account any user-specified overrides
     * to things like the maximum number of nodes that should be requested. In the latter case, it
     * limits the number of nodes requested to the number of subtasks to run, if that number is
     * smaller than the maximum number of nodes to request (that's what makes it a maximum number
     * rather than just a number).
     */
    private void computeNodeRequest(PipelineDefinitionNodeExecutionResources executionResources,
        int totalSubtaskCount, double subtasksPerCore) {

        // Start by computing the "optimal" number of nodes -- the number of nodes
        // needed to ensure that all subtasks get processed in parallel immediately, with no
        // subtasks needing to wait for available resources.
        computeRequestedNodeCount(totalSubtaskCount, subtasksPerCore);

        // If this number is LARGER than what the user wants, defer to the user;
        // if it's SMALLER than what the user wants, take the smaller value, since
        // the larger value would simply result in idle nodes.
        if (executionResources.getMaxNodes() > 0) {
            requestedNodeCount = Math.min(executionResources.getMaxNodes(), requestedNodeCount);
        }
    }

    /**
     * Determines the needed number of subtasks per core. This can be given by the user, as an
     * override, or determined from the wall times for the longest subtask vs a typical subtask. The
     * override will only be used if it will not result in some subtasks not getting processed
     * during the job.
     */
    private double subtasksPerCore(PipelineDefinitionNodeExecutionResources executionResources,
        int totalSubtaskCount) {

        double wallTimeRatio = executionResources.getSubtaskMaxWallTimeHours()
            / executionResources.getSubtaskTypicalWallTimeHours();
        double subtasksPerCore = wallTimeRatio;

        // If the number of nodes is limited, compute how many subtasks per active core
        // are needed; if this number is larger, keep it
        if (executionResources.getMaxNodes() > 0) {
            subtasksPerCore = Math.max(subtasksPerCore, (double) totalSubtaskCount
                / (activeCoresPerNode * executionResources.getMaxNodes()));
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

    /**
     * Determines the number of nodes to request from PBS. This is the number of nodes that are
     * needed to run all of the subtasks, given the number of subtasks that are to be run serially
     * on each active core and the total number of subtasks.
     */
    private void computeRequestedNodeCount(int totalSubtaskCount, double subtasksPerCore) {

        double nodesNeeded = totalSubtaskCount / (subtasksPerCore * activeCoresPerNode);
        requestedNodeCount = (int) Math.ceil(Math.max(1, nodesNeeded));
    }

    /**
     * Determine the number of active cores per node. This is the number of cores that can run given
     * the user-specified requirement on the amount of RAM needed for each subtask; the active
     * number of cores is the lowest of: the total number of cores in the node; the number of GB of
     * RAM in the node divided by the number of GB needed per subtask; the number of subtasks in the
     * task.
     */
    public void computeActiveCoresPerNode(
        PipelineDefinitionNodeExecutionResources executionResources, int totalSubtaskCount) {
        if (!executionResources.isNodeSharing()) {
            activeCoresPerNode = 1;
            return;
        }
        double gigsPerNode = architecture.getGigsPerCore() * minCoresPerNode;
        activeCoresPerNode = (int) Math
            .min(Math.floor(gigsPerNode / executionResources.getGigsPerSubtask()), minCoresPerNode);
        activeCoresPerNode = Math.min(activeCoresPerNode, totalSubtaskCount);
    }

    /**
     * Computes the wall time needed to run the job, and sets the corresponding job queue. The wall
     * time is rounded up to the nearest quarter hour. If no queue has sufficient wall time for the
     * requested job, an {@link IllegalStateException} is thrown.
     * <p>
     * If the user has specified single subtask per node execution, and has specified that execution
     * times are scaled inversely to cores per node, this scaling will be applied to estimate the
     * wall time needed.
     */
    private void computeWallTimeAndQueue(
        PipelineDefinitionNodeExecutionResources executionResources, double subtasksPerCore) {

        double unpaddedWallTimeHours = unpaddedWallTime(executionResources, subtasksPerCore);
        double requestedWallTimeHours = roundToNearest(unpaddedWallTimeHours,
            WALL_TIME_ROUNDING_INTERVAL_HOURS);
        requestedWallTime = TimeFormatter.timeInHoursToStringHhMmSs(requestedWallTimeHours);

        if (StringUtils.isBlank(executionResources.getQueueName())) {
            List<RemoteQueueDescriptor> queues = descriptorsSortedByMaxTime(
                architecture.getRemoteCluster());
            for (RemoteQueueDescriptor queue : queues) {
                if (queue.getMaxWallTimeHours() >= requestedWallTimeHours) {
                    queueName = queue.getQueueName();
                    break;
                }
            }
            if (queueName == null) {
                throw new IllegalStateException("No queues can support requested wall time of "
                    + TimeFormatter.stripSeconds(requestedWallTime));
            }
        } else {
            RemoteQueueDescriptor descriptor = RemoteQueueDescriptor
                .fromQueueName(executionResources.getQueueName());
            if (descriptor.equals(RemoteQueueDescriptor.UNKNOWN)) {
                log.warn("Unable to determine max wall time for queue {}",
                    executionResources.getQueueName());
                queueName = executionResources.getQueueName();
            } else if (descriptor.equals(RemoteQueueDescriptor.RESERVED)) {
                queueName = executionResources.getQueueName();
            } else {
                if (descriptor.getMaxWallTimeHours() < requestedWallTimeHours) {
                    throw new IllegalStateException("Queue " + descriptor.getQueueName()
                        + " cannot support job with wall time of "
                        + TimeFormatter.stripSeconds(requestedWallTime));
                }
                queueName = descriptor.getQueueName();
            }
        }
    }

    /** Calculates the wall time required for the current task before any padding is added. */
    private double unpaddedWallTime(PipelineDefinitionNodeExecutionResources executionResources,
        double subtasksPerCore) {

        double subtaskMaxWallTimeHours = executionResources.getSubtaskMaxWallTimeHours();
        double wallTimeFromTypical = subtasksPerCore
            * executionResources.getSubtaskTypicalWallTimeHours();

        if (!executionResources.isNodeSharing() && executionResources.isWallTimeScaling()) {
            // If we're using internal parallelization, we can scale down the wall time by the
            // cores per node.
            subtaskMaxWallTimeHours /= minCoresPerNode;
            wallTimeFromTypical /= minCoresPerNode;
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
     * Estimates the job cost in the native units of the relevant cluster (SBUs, dollars, or other
     * units). The estimate takes into account the cost factor of the architecture, the scaling of
     * the cost factor for the selection of a cores-per-node count that is higher than the minimum,
     * the number of nodes, and the wall time request. The estimate does not consider inflation due
     * to PBS selecting nodes with core counts higher than the requested minimum, nor due to PBS
     * selecting nodes with bandwidth and/or storage capabilities that are higher than those of the
     * least expensive sub-architecture.
     */
    private void computeEstimatedCost() {

        double costFactor = architecture.getCostFactor();
        estimatedCost = costFactor * minCoresPerNode / architecture.getMinCores()
            * requestedNodeCount * TimeFormatter.timeStringHhMmSsToTimeInHours(requestedWallTime);
    }

    /**
     * Aggregates PBS parameters across a {@link Collection} of parameter instances. For most
     * parameters the value in the aggregated parameter instance is the same as the value in the
     * last of the collection to be looped over, and it is assumed that all the members of the
     * collection have the same value for these parameters (i.e., all of the instances in the
     * collection have the same architecture, remote group, etc.). In the case of the wall time and
     * the active cores per node, the aggregated value is the largest value across all instances in
     * the collection. In the case of the queue name, the aggregated value is the queue with the
     * longest maximum time. In the case of the number of nodes and the estimated cost, the
     * aggregated value is the sum across all instances in the collection.
     * <p>
     * This method allows the estimation of the overall cost and parameters for running a given
     * pipeline module, given the parameters for each task that will run in the module.
     */
    public static PbsParameters aggregatePbsParameters(
        Collection<PbsParameters> pbsParametersCollection) {
        PbsParameters aggregatedParameters = new PbsParameters();

        // Loop over PbsParameters instances and collect values.
        for (PbsParameters pbsParameters : pbsParametersCollection) {

            // Start with the fields that are the same for all members of the collection.
            aggregatedParameters.setEnabled(pbsParameters.isEnabled());
            aggregatedParameters.setMinGigsPerNode(pbsParameters.getMinGigsPerNode());
            aggregatedParameters.setMinCoresPerNode(pbsParameters.getMinCoresPerNode());
            aggregatedParameters.setGigsPerSubtask(pbsParameters.getGigsPerSubtask());
            aggregatedParameters.setRemoteGroup(pbsParameters.getRemoteGroup());
            aggregatedParameters.setArchitecture(pbsParameters.getArchitecture());
            aggregatedParameters.setRemoteGroup(pbsParameters.getRemoteGroup());

            // Next the fields where we need to find the maximum value across all
            // members of the collection.
            double wallTimeHours = StringUtils.isBlank(aggregatedParameters.getRequestedWallTime())
                ? 0
                : TimeFormatter
                    .timeStringHhMmSsToTimeInHours(aggregatedParameters.getRequestedWallTime());
            wallTimeHours = Math.max(wallTimeHours,
                TimeFormatter.timeStringHhMmSsToTimeInHours(pbsParameters.getRequestedWallTime()));
            aggregatedParameters
                .setRequestedWallTime(TimeFormatter.timeInHoursToStringHhMmSs(wallTimeHours));
            aggregatedParameters
                .setActiveCoresPerNode(Math.max(aggregatedParameters.getActiveCoresPerNode(),
                    pbsParameters.getActiveCoresPerNode()));
            RemoteQueueDescriptor pbsQueue = RemoteQueueDescriptor
                .fromQueueName(pbsParameters.getQueueName());
            RemoteQueueDescriptor aggregatorQueue = StringUtils
                .isBlank(aggregatedParameters.getQueueName()) ? pbsQueue
                    : RemoteQueueDescriptor.fromQueueName(aggregatedParameters.getQueueName());
            RemoteQueueDescriptor longerQueue = RemoteQueueDescriptor.max(pbsQueue,
                aggregatorQueue);
            aggregatedParameters.setQueueName(longerQueue.getQueueName());

            // Finally the fields where we need to sum across the collection.
            aggregatedParameters.setRequestedNodeCount(aggregatedParameters.getRequestedNodeCount()
                + pbsParameters.getRequestedNodeCount());
            aggregatedParameters.setEstimatedCost(
                aggregatedParameters.getEstimatedCost() + pbsParameters.getEstimatedCost());
        }
        return aggregatedParameters;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getRequestedWallTime() {
        return requestedWallTime;
    }

    public void setRequestedWallTime(String requestedWallTime) {
        this.requestedWallTime = requestedWallTime;
    }

    public int getRequestedNodeCount() {
        return requestedNodeCount;
    }

    public void setRequestedNodeCount(int requestedNodeCount) {
        this.requestedNodeCount = requestedNodeCount;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public double getMinGigsPerNode() {
        return minGigsPerNode;
    }

    public void setMinGigsPerNode(double minGigsPerNode) {
        this.minGigsPerNode = minGigsPerNode;
    }

    public int getMinCoresPerNode() {
        return minCoresPerNode;
    }

    public void setMinCoresPerNode(int minCoresPerNode) {
        this.minCoresPerNode = minCoresPerNode;
    }

    public double getGigsPerSubtask() {
        return gigsPerSubtask;
    }

    public void setGigsPerSubtask(double gigsPerSubtask) {
        this.gigsPerSubtask = gigsPerSubtask;
    }

    public String getRemoteGroup() {
        return remoteGroup;
    }

    public void setRemoteGroup(String remoteGroup) {
        this.remoteGroup = remoteGroup;
    }

    public RemoteNodeDescriptor getArchitecture() {
        return architecture;
    }

    public void setArchitecture(RemoteNodeDescriptor architecture) {
        this.architecture = architecture;
    }

    public int getActiveCoresPerNode() {
        return activeCoresPerNode;
    }

    public void setActiveCoresPerNode(int activeCoresPerNode) {
        this.activeCoresPerNode = activeCoresPerNode;
    }

    public double getEstimatedCost() {
        return estimatedCost;
    }

    public void setEstimatedCost(double estimatedCost) {
        this.estimatedCost = estimatedCost;
    }
}
