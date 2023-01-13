package gov.nasa.ziggy.module.remote;

import static gov.nasa.ziggy.module.remote.RemoteNodeDescriptor.descriptorsSortedByCores;
import static gov.nasa.ziggy.module.remote.RemoteNodeDescriptor.descriptorsSortedByCost;
import static gov.nasa.ziggy.module.remote.RemoteNodeDescriptor.descriptorsSortedByRamThenCost;
import static gov.nasa.ziggy.module.remote.RemoteNodeDescriptor.nodeHasSufficientRam;
import static gov.nasa.ziggy.module.remote.RemoteNodeDescriptor.nodesWithSufficientRam;
import static gov.nasa.ziggy.module.remote.RemoteQueueDescriptor.descriptorsSortedByMaxTime;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.util.TimeFormatter;

/**
 * Parameters needed to submit a job via the PBS batch system. Parameters are derived from an
 * appropriate {@link RemoteParameters} instance. Any optional parameters will be determined by the
 * needs of the job as determined from the required parameters.
 *
 * @author PT
 */
public class PbsParameters {

    private boolean enabled;
    private String requestedWallTime;
    private int requestedNodeCount;
    private String queueName;
    private int minGigsPerNode;
    private int minCoresPerNode;
    private double gigsPerSubtask;
    private String remoteGroup;
    private RemoteNodeDescriptor architecture;
    private int activeCoresPerNode;
    private double estimatedCost;

    private static final Logger log = LoggerFactory.getLogger(PbsParameters.class);

    /**
     * Populates the architecture property of the {@link PbsParameters} instance. If the
     * architecture is not specified already, it is selected by use of the architecture optimization
     * option in {@link RemoteParameters}. If it is specified, the architecture is nonetheless
     * checked to ensure that there is sufficient RAM on the selected architecture to run at least 1
     * subtask per node.
     */
    public void populateArchitecture(RemoteParameters remoteParameters, int totalSubtasks,
        SupportedRemoteClusters remoteCluster) {

        // If the architecture isn't specified, determine it via optimization.
        if (getArchitecture() == null) {
            log.info("Selecting infrastructure for " + totalSubtasks + " subtasks using optimizer "
                + remoteParameters.getOptimizer());
            selectArchitecture(remoteParameters, totalSubtasks, remoteCluster);
        }

        // Note that if the architecture IS specified, it may still not be compatible
        // with the RAM requirements; check that possibility now.
        if (remoteParameters.isNodeSharing()
            && !nodeHasSufficientRam(getArchitecture(), remoteParameters.getGigsPerSubtask())) {
            throw new IllegalStateException("selected node architecture "
                + getArchitecture().toString() + " has insufficient RAM for subtasks that require "
                + remoteParameters.getGigsPerSubtask() + " GB");
        }
    }

    /**
     * Selects an architecture using an optimizer option. The architecture will be one of the ones
     * that corresponds to the {@link SupportedRemoteCluster}, and will be selected from the ones
     * with sufficient RAM to perform the processing.
     */
    private void selectArchitecture(RemoteParameters remoteParameters, int totalSubtasks,
        SupportedRemoteClusters remoteCluster) {

        double gigsPerSubtask = remoteParameters.getGigsPerSubtask();
        RemoteArchitectureOptimizer optimizer = RemoteArchitectureOptimizer
            .fromName(remoteParameters.getOptimizer());
        List<RemoteNodeDescriptor> acceptableNodes = null;
        if (!remoteParameters.isNodeSharing()) {
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
                    "All remote architectures have insufficient RAM to support " + gigsPerSubtask
                        + " GB per subtask requirement");
            }
        }

        architecture = optimizer.optimalArchitecture(remoteParameters, totalSubtasks,
            acceptableNodes);
    }

    /**
     * Populates the resource-related parameters: specifically, the number of nodes, the number of
     * subtasks per node, the wall time request, the queue, and the remote group to be billed for
     * the PBS jobs. An estimate of the cost (in SBUs or dollars) is also performed.
     */
    public void populateResourceParameters(RemoteParameters remoteParameters,
        int totalSubtaskCount) {

        computeActiveCoresPerNode(remoteParameters);
        double subtasksPerCore = subtasksPerCore(remoteParameters, totalSubtaskCount);

        // Set the number of nodes and the number of subtasks per node
        computeNodeRequest(remoteParameters, totalSubtaskCount, subtasksPerCore);

        // Set the wall time and the queue
        computeWallTimeAndQueue(remoteParameters, subtasksPerCore);

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
    private void computeNodeRequest(RemoteParameters remoteParameters, int totalSubtaskCount,
        double subtasksPerCore) {

        // Start by computing the "optimal" number of nodes -- the number of nodes
        // needed
        // to ensure that all subtasks get processed in parallel immediately, with no
        // subtasks needing to wait for available resources.
        computeRequestedNodeCount(totalSubtaskCount, subtasksPerCore);

        // If this number is LARGER than what the user wants, defer to the user;
        // if it's SMALLER than what the user wants, take the smaller value, since
        // the larger value would simply result in idle nodes.
        if (!StringUtils.isEmpty(remoteParameters.getMaxNodes())) {
            requestedNodeCount = Math.min(Integer.parseInt(remoteParameters.getMaxNodes()),
                requestedNodeCount);
        }
    }

    /**
     * Determines the needed number of subtasks per core. This can be given by the user, as an
     * override, or determined from the wall times for the longest subtask vs a typical subtask. The
     * override will only be used if it will not result in some subtasks not getting processed
     * during the job.
     */
    private double subtasksPerCore(RemoteParameters remoteParameters, int totalSubtaskCount) {

        double wallTimeRatio = remoteParameters.getSubtaskMaxWallTimeHours()
            / remoteParameters.getSubtaskTypicalWallTimeHours();
        double subtasksPerCore = wallTimeRatio;

        // If the number of nodes is limited, compute how many subtasks per active core
        // are needed; if this number is larger, keep it
        if (!StringUtils.isEmpty(remoteParameters.getMaxNodes())) {
            subtasksPerCore = Math.max(subtasksPerCore, (double) totalSubtaskCount
                / (activeCoresPerNode * Integer.parseInt(remoteParameters.getMaxNodes())));
        }

        // Finally, if the user has supplied an override for subtasks per core, apply it
        // unless it conflicts with the value needed to make the job complete
        if (!StringUtils.isEmpty(remoteParameters.getSubtasksPerCore())) {
            double overrideSubtasksPerCore = Double
                .parseDouble(remoteParameters.getSubtasksPerCore());
            if (overrideSubtasksPerCore >= subtasksPerCore) {
                subtasksPerCore = overrideSubtasksPerCore;
            } else {
                log.warn("User-supplied subtasks per core " + overrideSubtasksPerCore
                    + " too small, using algorithmic value of " + subtasksPerCore);
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
     * number of cores is the lower of: the total number of cores in the node, or the number of GB
     * of RAM in the node divided by the number of GB needed per subtask.
     */
    public void computeActiveCoresPerNode(RemoteParameters remoteParameters) {
        if (!remoteParameters.isNodeSharing()) {
            activeCoresPerNode = 1;
            return;
        }
        double gigsPerNode = architecture.getGigsPerCore() * minCoresPerNode;
        activeCoresPerNode = (int) Math
            .min(Math.floor(gigsPerNode / remoteParameters.getGigsPerSubtask()), minCoresPerNode);
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
    private void computeWallTimeAndQueue(RemoteParameters remoteParameters,
        double subtasksPerCore) {

        double typicalWallTimeHours = remoteParameters.getSubtaskTypicalWallTimeHours();
        double bareWallTimeHours = Math.max(remoteParameters.getSubtaskMaxWallTimeHours(),
            subtasksPerCore * typicalWallTimeHours);
        if (!remoteParameters.isNodeSharing() && remoteParameters.isWallTimeScaling()) {
            bareWallTimeHours /= minCoresPerNode;
        }
        double requestedWallTimeHours = 0.25 * Math.ceil(4 * bareWallTimeHours);
        requestedWallTime = TimeFormatter.timeInHoursToStringHhMmSs(requestedWallTimeHours);

        if (StringUtils.isEmpty(remoteParameters.getQueueName())) {
            List<RemoteQueueDescriptor> queues = descriptorsSortedByMaxTime(
                architecture.getRemoteCluster());
            for (RemoteQueueDescriptor queue : queues) {
                if (queue.getMaxWallTimeHours() >= requestedWallTimeHours) {
                    queueName = queue.getQueueName();
                    break;
                }
            }
            if (queueName == null) {
                throw new IllegalStateException(
                    "No queues can support requested wall time of " + requestedWallTime);
            }
        } else {
            RemoteQueueDescriptor descriptor = RemoteQueueDescriptor
                .fromName(remoteParameters.getQueueName());
            if (descriptor.equals(RemoteQueueDescriptor.UNKNOWN)) {
                log.warn("Unable to determine max wall time for queue "
                    + remoteParameters.getQueueName());
                queueName = remoteParameters.getQueueName();
            } else {
                if (descriptor.getMaxWallTimeHours() < requestedWallTimeHours) {
                    throw new IllegalStateException("Queue " + descriptor.getQueueName()
                        + " cannot support job with wall time of " + requestedWallTime);
                }
                queueName = descriptor.getQueueName();
            }
        }
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

    public int getMinGigsPerNode() {
        return minGigsPerNode;
    }

    public void setMinGigsPerNode(int minGigsPerNode) {
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
