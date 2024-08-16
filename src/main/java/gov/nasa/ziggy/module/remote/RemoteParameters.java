package gov.nasa.ziggy.module.remote;

import org.apache.commons.lang3.StringUtils;

/**
 * User-facing parameters that control remote execution (on the NAS, AWS, or other large-scale batch
 * processing system). The user is required to supply several parameters, such as the wall times
 * needed by subtasks. Other parameters are optional and the user can allow the pipeline
 * infrastructure to determine the optimal value for these parameters.
 *
 * @author PT
 */
public class RemoteParameters {

    /** Do you want to run remote at all? */
    private boolean enabled;

    /** longest wall time needed by any subtask */
    private double subtaskMaxWallTimeHours;

    /** typical (median) wall time needed for a subtask */
    private double subtaskTypicalWallTimeHours;

    /** RAM needed by each subtask, in GB */
    private double gigsPerSubtask;

    /** Minimum number of subtasks that must be present for remote execution to be used. */
    private int minSubtasksForRemoteExecution = 0;

    private String remoteNodeArchitecture;

    /** job queue to use */
    private String queueName;

    /** number of subtasks per active core */
    private String subtasksPerCore;

    /** Maximum number of nodes to select */
    private String maxNodes;

    /** minimum cores per node (AWS only) */
    private String minCoresPerNode;

    /** minimum RAM per node in GB (AWS only) */
    private String minGigsPerNode;

    /** Which optimizer to use in selecting an architecture */
    private String optimizer = "CORES";

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
        minSubtasksForRemoteExecution = original.minSubtasksForRemoteExecution;
        remoteNodeArchitecture = original.remoteNodeArchitecture;
        queueName = original.queueName;
        subtasksPerCore = original.subtasksPerCore;
        minCoresPerNode = original.minCoresPerNode;
        minGigsPerNode = original.minGigsPerNode;
        optimizer = original.optimizer;
        nodeSharing = original.nodeSharing;
        wallTimeScaling = original.wallTimeScaling;
    }

    public PbsParameters pbsParametersInstance() {
        PbsParameters pbsParameters = new PbsParameters();
        pbsParameters.setEnabled(enabled);
        pbsParameters.setArchitecture(RemoteNodeDescriptor.fromName(remoteNodeArchitecture));
        pbsParameters.setGigsPerSubtask(gigsPerSubtask);
        if (!StringUtils.isBlank(queueName)) {
            pbsParameters.setQueueName(queueName);
        }
        if (!StringUtils.isBlank(minCoresPerNode)) {
            pbsParameters.setMinCoresPerNode(Integer.parseInt(minCoresPerNode));
        }
        if (!StringUtils.isBlank(minGigsPerNode)) {
            pbsParameters.setMinGigsPerNode(Integer.parseInt(minGigsPerNode));
        }
        if (!StringUtils.isBlank(maxNodes)) {
            pbsParameters.setRequestedNodeCount(Integer.parseInt(maxNodes));
        }
        return pbsParameters;
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
        return StringUtils.isBlank(optimizer) ? "CORES" : optimizer;
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
