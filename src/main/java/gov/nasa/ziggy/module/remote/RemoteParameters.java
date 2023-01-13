package gov.nasa.ziggy.module.remote;

import static com.google.common.base.Preconditions.checkState;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.StringUtils;

import gov.nasa.ziggy.parameters.Parameters;

/**
 * User-facing parameters that control remote execution (on the NAS, AWS, or other large-scale batch
 * processing system). The user is required to supply several parameters, such as the wall times
 * needed by subtasks. Other parameters are optional and the user can allow the pipeline
 * infrastructure to determine the optimal value for these parameters.
 *
 * @author PT
 */
public class RemoteParameters implements Parameters {

    public RemoteParameters() {

    }

    // REQUIRED parameters
    private boolean enabled;
    /** Do you want to run remote at all? */

    private double subtaskMaxWallTimeHours;
    /** longest wall time needed by any subtask */

    private double subtaskTypicalWallTimeHours;
    /** typical (median) wall time needed for a subtask */

    private double gigsPerSubtask;
    /** RAM needed by each subtask, in GB */

    private int minSubtasksForRemoteExecution = 0;
    /**
     * Minimum number of subtasks that must be present for remote execution to be used.
     */

    // OPTIONAL parameters -- these are all Strings so that they can be empty,
    // and can appear empty on the parameters GUI. Non-empty instances will be checked
    // to make sure they can be converted to appropriate numeric values in validate().
    private String remoteNodeArchitecture;
    /** Type of node to use */

    private String queueName;
    /** job queue to use */

    private String subtasksPerCore;
    /** number of subtasks per active core */

    private String maxNodes;
    /** Maximum number of nodes to select */

    private String minCoresPerNode;
    /** minimum cores per node (AWS only) */

    private String minGigsPerNode;
    /** minimum RAM per node in GB (AWS only) */

    private String optimizer = "CORES";
    /** Which optimizer to use in selecting an architecture */

    /**
     * Allow a node to process multiple subtasks in parallel. This is the standard mode of
     * processing for algorithms that don't provide their own concurrency support that can be used
     * to spread the computational load of the algorithm across multiple cores.
     */
    private boolean nodeSharing = true;

    /**
     * Scale user-supplied wall times inversely to the number of cores per node. This option is only
     * used when {@link #oneSubtaskPerNode} is true, and indicates that the algorithm code has its
     * own concurrency support that allows all the cores in a node to be utilized when that node is
     * processing a single subtask.
     */
    private boolean wallTimeScaling = true;

    /**
     * Copy constructor.
     */
    public RemoteParameters(RemoteParameters original) {
        enabled = original.enabled;
        subtaskMaxWallTimeHours = original.subtaskMaxWallTimeHours;
        subtaskTypicalWallTimeHours = original.subtaskTypicalWallTimeHours;
        gigsPerSubtask = original.gigsPerSubtask;

        remoteNodeArchitecture = copyOrEmptyString(original.remoteNodeArchitecture);
        queueName = copyOrEmptyString(original.queueName);
        subtasksPerCore = copyOrEmptyString(original.subtasksPerCore);
        maxNodes = copyOrEmptyString(original.maxNodes);
        minCoresPerNode = copyOrEmptyString(original.minCoresPerNode);
        minGigsPerNode = copyOrEmptyString(original.minGigsPerNode);
        optimizer = copyOrEmptyString(original.optimizer);

        nodeSharing = original.nodeSharing;
        wallTimeScaling = original.wallTimeScaling;
    }

    private String copyOrEmptyString(String s1) {
        return s1 != null ? new String(s1) : new String();
    }

    public PbsParameters pbsParametersInstance() {
        PbsParameters pbsParameters = new PbsParameters();
        pbsParameters.setEnabled(enabled);
        pbsParameters.setArchitecture(RemoteNodeDescriptor.fromName(remoteNodeArchitecture));
        pbsParameters.setGigsPerSubtask(gigsPerSubtask);
        if (!StringUtils.isEmpty(queueName)) {
            pbsParameters.setQueueName(queueName);
        }
        if (!StringUtils.isEmpty(minCoresPerNode)) {
            pbsParameters.setMinCoresPerNode(Integer.parseInt(minCoresPerNode));
        }
        if (!StringUtils.isEmpty(minGigsPerNode)) {
            pbsParameters.setMinGigsPerNode(Integer.parseInt(minGigsPerNode));
        }
        if (!StringUtils.isEmpty(maxNodes)) {
            pbsParameters.setRequestedNodeCount(Integer.parseInt(maxNodes));
        }
        return pbsParameters;
    }

    @Override
    public void validate() {
        checkState(subtaskMaxWallTimeHours > 0, "subtaskMaxWallTimeHours must > 0");
        checkState(subtaskTypicalWallTimeHours <= subtaskMaxWallTimeHours,
            "Typical wall time must be <= maximum wall time");
        checkState(gigsPerSubtask > 0, "gigsPerSubtask must be > 0");

        checkState(
            subtasksPerCore.isEmpty()
                || NumberUtils.isNumber(subtasksPerCore) && Double.parseDouble(subtasksPerCore) > 0,
            "subtasksPerCore must be > 0");
        checkState(
            maxNodes.isEmpty() || NumberUtils.isNumber(maxNodes) && Integer.parseInt(maxNodes) > 0,
            "maxNodes must be > 0");
        checkState(
            minCoresPerNode.isEmpty()
                || NumberUtils.isNumber(minCoresPerNode) && Integer.parseInt(minCoresPerNode) > 0,
            "minCoresPerNode must be > 0");
        checkState(
            minGigsPerNode.isEmpty()
                || NumberUtils.isNumber(minGigsPerNode) && Double.parseDouble(minGigsPerNode) > 0,
            "minGigsPerNode must be > 0");
        checkState(queueName.isEmpty() || RemoteQueueDescriptor.fromName(queueName) != null,
            "Queue name not recognized: " + queueName);
        checkState(
            remoteNodeArchitecture.isEmpty()
                || RemoteNodeDescriptor.fromName(remoteNodeArchitecture) != null,
            "Architecture not recognized: " + remoteNodeArchitecture);

        checkState(RemoteArchitectureOptimizer.fromName(optimizer) != null,
            "Optimizer must be one of " + RemoteArchitectureOptimizer.options());

        if (!remoteNodeArchitecture.isEmpty() && !queueName.isEmpty()) {
            checkState(checkArchitectureAndQueue(), "Architecture " + remoteNodeArchitecture
                + " and queue " + queueName + " not consistent");
        }
    }

    private boolean checkArchitectureAndQueue() {
        RemoteNodeDescriptor node = RemoteNodeDescriptor.fromName(remoteNodeArchitecture);
        RemoteQueueDescriptor queue = RemoteQueueDescriptor.fromName(queueName);
        return node.getRemoteCluster().equals(queue.getRemoteCluster());
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public double getSubtaskMaxWallTimeHours() {
        return subtaskMaxWallTimeHours;
    }

    public void setSubtaskMaxWallTimeHours(double subtaskMaxWallTimeHours) {
        this.subtaskMaxWallTimeHours = subtaskMaxWallTimeHours;
    }

    public double getSubtaskTypicalWallTimeHours() {
        return subtaskTypicalWallTimeHours;
    }

    public void setSubtaskTypicalWallTimeHours(double subtaskTypicalWallTimeHours) {
        this.subtaskTypicalWallTimeHours = subtaskTypicalWallTimeHours;
    }

    public double getGigsPerSubtask() {
        return gigsPerSubtask;
    }

    public void setGigsPerSubtask(double gigsPerSubtask) {
        this.gigsPerSubtask = gigsPerSubtask;
    }

    public String getRemoteNodeArchitecture() {
        return remoteNodeArchitecture;
    }

    public void setRemoteNodeArchitecture(String remoteNodeArchitecture) {
        this.remoteNodeArchitecture = remoteNodeArchitecture;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getSubtasksPerCore() {
        return subtasksPerCore;
    }

    public void setSubtasksPerCore(String subtasksPerCore) {
        this.subtasksPerCore = subtasksPerCore;
    }

    public String getMaxNodes() {
        return maxNodes;
    }

    public void setMaxNodes(String maxNodes) {
        this.maxNodes = maxNodes;
    }

    public String getMinCoresPerNode() {
        return minCoresPerNode;
    }

    public void setMinCoresPerNode(String minCoresPerNode) {
        this.minCoresPerNode = minCoresPerNode;
    }

    public String getMinGigsPerNode() {
        return minGigsPerNode;
    }

    public void setMinGigsPerNode(String minGigsPerNode) {
        this.minGigsPerNode = minGigsPerNode;
    }

    public String getOptimizer() {
        return StringUtils.isEmpty(optimizer) ? "CORES" : optimizer;
    }

    public void setOptimizer(String optimizer) {
        this.optimizer = optimizer;
    }

    public boolean isNodeSharing() {
        return nodeSharing;
    }

    public void setNodeSharing(boolean nodeSharing) {
        this.nodeSharing = nodeSharing;
    }

    public boolean isWallTimeScaling() {
        return wallTimeScaling;
    }

    public void setWallTimeScaling(boolean wallTimeScaling) {
        this.wallTimeScaling = wallTimeScaling;
    }

    public int getMinSubtasksForRemoteExecution() {
        return minSubtasksForRemoteExecution;
    }

    public void setMinSubtasksForRemoteExecution(int minSubtasksForRemoteExecution) {
        this.minSubtasksForRemoteExecution = minSubtasksForRemoteExecution;
    }

}
