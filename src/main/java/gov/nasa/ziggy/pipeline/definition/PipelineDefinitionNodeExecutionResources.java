package gov.nasa.ziggy.pipeline.definition;

import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

import gov.nasa.ziggy.module.remote.PbsParameters;
import gov.nasa.ziggy.module.remote.RemoteArchitectureOptimizer;
import gov.nasa.ziggy.module.remote.RemoteNodeDescriptor;
import gov.nasa.ziggy.worker.WorkerResources;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

/**
 * Parameters relevant for configuring execution of a {@link PipelineDefinitionNode}. The
 * configuration is related to a specific {@link PipelineDefinitionNode} via fields that contain the
 * node's module name and pipeline name. This ensures that a given
 * {@link PipelineDefinitionNodeExecutionResources} is associated with any and all versions of its
 * definition node and that none of these parameters are involved in determining whether a node
 * definition is up to date (which would be the case if the class was embedded).
 *
 * @author PT
 */

@Entity
@Table(name = "ziggy_PipelineDefinitionNode_executionResources", uniqueConstraints = {
    @UniqueConstraint(columnNames = { "pipelineName", "pipelineModuleName" }) })
public class PipelineDefinitionNodeExecutionResources {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE,
        generator = "ziggy_PipelineDefinitionNode_executionResources_generator")
    @SequenceGenerator(name = "ziggy_PipelineDefinitionNode_executionResources_generator",
        initialValue = 1, sequenceName = "ziggy_PipelineDefinitionNode_executionResources_sequence",
        allocationSize = 1)
    private Long id;

    // Fields that provide mapping to a specific pipeline definition.
    private final String pipelineName;
    private final String pipelineModuleName;

    // Fields that control worker-side execution resource options.
    private int maxWorkerCount = 0;
    private int heapSizeMb = 0;
    private int maxFailedSubtaskCount = 0;
    private int maxAutoResubmits = 0;

    // Fields that control remote execution and are mandatory.
    private boolean remoteExecutionEnabled = false;
    private double subtaskMaxWallTimeHours = 0;
    private double subtaskTypicalWallTimeHours = 0;
    private double gigsPerSubtask = 0;
    @Enumerated(EnumType.STRING)
    private RemoteArchitectureOptimizer optimizer = RemoteArchitectureOptimizer.COST;
    private boolean nodeSharing = true;
    private boolean wallTimeScaling = true;

    // Optional parameters for remote execution. The user can specify these, or can
    // allow the remote execution calculator decide the values. For the Strings, an
    // empty string indicates that the user has not set a value. For the int and double
    // primitives, this is indicated by a zero; the exception is minSubtasksForRemoteExecution,
    // in which -1 indicates that the user has not entered a value.
    private String remoteNodeArchitecture = "";
    private String queueName = "";
    private int maxNodes;
    private double subtasksPerCore;
    private int minCoresPerNode;
    private double minGigsPerNode;
    private int minSubtasksForRemoteExecution = -1;

    // "The JPA specification requires that all persistent classes have a no-arg constructor. This
    // constructor may be public or protected."
    protected PipelineDefinitionNodeExecutionResources() {
        this("", "");
    }

    public PipelineDefinitionNodeExecutionResources(String pipelineName,
        String pipelineModuleName) {
        this.pipelineName = pipelineName;
        this.pipelineModuleName = pipelineModuleName;
    }

    /** Copy constructor. */
    public PipelineDefinitionNodeExecutionResources(
        PipelineDefinitionNodeExecutionResources original) {
        this(original.pipelineName, original.pipelineModuleName);
        populateFrom(original);
    }

    /**
     * Populates one instance with the values of another. This is useful for quickly putting values
     * from a copied instance back into the original. This allows the original to be merged back
     * into the database.
     */
    public void populateFrom(PipelineDefinitionNodeExecutionResources other) {
        heapSizeMb = other.heapSizeMb;
        maxWorkerCount = other.maxWorkerCount;
        maxFailedSubtaskCount = other.maxFailedSubtaskCount;
        maxAutoResubmits = other.maxAutoResubmits;
        remoteExecutionEnabled = other.remoteExecutionEnabled;
        subtaskMaxWallTimeHours = other.subtaskMaxWallTimeHours;
        subtaskTypicalWallTimeHours = other.subtaskTypicalWallTimeHours;
        gigsPerSubtask = other.gigsPerSubtask;
        minSubtasksForRemoteExecution = other.minSubtasksForRemoteExecution;
        optimizer = other.optimizer;
        nodeSharing = other.nodeSharing;
        wallTimeScaling = other.wallTimeScaling;

        remoteNodeArchitecture = other.remoteNodeArchitecture;
        queueName = other.queueName;
        maxNodes = other.maxNodes;
        subtasksPerCore = other.subtasksPerCore;
        minCoresPerNode = other.minCoresPerNode;
        minGigsPerNode = other.minGigsPerNode;
    }

    public PbsParameters pbsParametersInstance() {
        PbsParameters pbsParameters = new PbsParameters();
        pbsParameters.setEnabled(remoteExecutionEnabled);
        pbsParameters.setArchitecture(RemoteNodeDescriptor.fromName(remoteNodeArchitecture));
        pbsParameters.setGigsPerSubtask(gigsPerSubtask);
        if (!StringUtils.isEmpty(queueName)) {
            pbsParameters.setQueueName(queueName);
        }
        if (minCoresPerNode > 0) {
            pbsParameters.setMinCoresPerNode(minCoresPerNode);
        }
        if (minGigsPerNode > 0) {
            pbsParameters.setMinGigsPerNode(minGigsPerNode);
        }
        if (maxNodes > 0) {
            pbsParameters.setRequestedNodeCount(maxNodes);
        }
        return pbsParameters;
    }

    /**
     * Returns the worker resources for the current node. If resources are not specified, the
     * resources object will have nulls, in which case default values will be retrieved from the
     * {@link WorkerResources} singleton when the object is queried.
     */
    public WorkerResources workerResources() {
        Integer workerCount = maxWorkerCount <= 0 ? null : maxWorkerCount;
        Integer heapSize = heapSizeMb <= 0 ? null : heapSizeMb;
        return new WorkerResources(workerCount, heapSize);
    }

    /**
     * Applies the worker resources values to a pipeline instance node. If the values are the
     * default ones, the node's values will be set to zero rather than the values returned by the
     * resources object.
     */
    public void applyWorkerResources(WorkerResources resources) {
        maxWorkerCount = resources.getMaxWorkerCount() == null ? 0 : resources.getMaxWorkerCount();
        heapSizeMb = resources.getHeapSizeMb() == null ? 0 : resources.getHeapSizeMb();
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public String getPipelineModuleName() {
        return pipelineModuleName;
    }

    public int getMaxWorkerCount() {
        return maxWorkerCount;
    }

    public void setMaxWorkerCount(int maxWorkerCount) {
        this.maxWorkerCount = maxWorkerCount;
    }

    public int getHeapSizeMb() {
        return heapSizeMb;
    }

    public void setHeapSizeMb(int heapSizeMb) {
        this.heapSizeMb = heapSizeMb;
    }

    public int getMaxFailedSubtaskCount() {
        return maxFailedSubtaskCount;
    }

    public void setMaxFailedSubtaskCount(int maxFailedSubtaskCount) {
        this.maxFailedSubtaskCount = maxFailedSubtaskCount;
    }

    public int getMaxAutoResubmits() {
        return maxAutoResubmits;
    }

    public void setMaxAutoResubmits(int maxAutoResubmits) {
        this.maxAutoResubmits = maxAutoResubmits;
    }

    public boolean isRemoteExecutionEnabled() {
        return remoteExecutionEnabled;
    }

    public void setRemoteExecutionEnabled(boolean remoteExecutionEnabled) {
        this.remoteExecutionEnabled = remoteExecutionEnabled;
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

    public int getMinSubtasksForRemoteExecution() {
        return minSubtasksForRemoteExecution;
    }

    public void setMinSubtasksForRemoteExecution(int minSubtasksForRemoteExecution) {
        this.minSubtasksForRemoteExecution = minSubtasksForRemoteExecution;
    }

    public RemoteArchitectureOptimizer getOptimizer() {
        return optimizer;
    }

    public void setOptimizer(RemoteArchitectureOptimizer optimizer) {
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

    public int getMaxNodes() {
        return maxNodes;
    }

    public void setMaxNodes(int maxNodes) {
        this.maxNodes = maxNodes;
    }

    public double getSubtasksPerCore() {
        return subtasksPerCore;
    }

    public void setSubtasksPerCore(double subtasksPerCore) {
        this.subtasksPerCore = subtasksPerCore;
    }

    public int getMinCoresPerNode() {
        return minCoresPerNode;
    }

    public void setMinCoresPerNode(int minCoresPerNode) {
        this.minCoresPerNode = minCoresPerNode;
    }

    public double getMinGigsPerNode() {
        return minGigsPerNode;
    }

    public void setMinGigsPerNode(double minGigsPerNode) {
        this.minGigsPerNode = minGigsPerNode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(gigsPerSubtask, maxNodes, minCoresPerNode, minGigsPerNode,
            minSubtasksForRemoteExecution, nodeSharing, optimizer, queueName,
            remoteExecutionEnabled, remoteNodeArchitecture, subtaskMaxWallTimeHours,
            subtaskTypicalWallTimeHours, subtasksPerCore, wallTimeScaling);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PipelineDefinitionNodeExecutionResources other = (PipelineDefinitionNodeExecutionResources) obj;
        return Double.doubleToLongBits(gigsPerSubtask) == Double
            .doubleToLongBits(other.gigsPerSubtask) && Objects.equals(maxNodes, other.maxNodes)
            && Objects.equals(minCoresPerNode, other.minCoresPerNode)
            && Objects.equals(minGigsPerNode, other.minGigsPerNode)
            && minSubtasksForRemoteExecution == other.minSubtasksForRemoteExecution
            && nodeSharing == other.nodeSharing && optimizer == other.optimizer
            && Objects.equals(queueName, other.queueName)
            && remoteExecutionEnabled == other.remoteExecutionEnabled
            && Objects.equals(remoteNodeArchitecture, other.remoteNodeArchitecture)
            && Double.doubleToLongBits(subtaskMaxWallTimeHours) == Double
                .doubleToLongBits(other.subtaskMaxWallTimeHours)
            && Double.doubleToLongBits(subtaskTypicalWallTimeHours) == Double
                .doubleToLongBits(other.subtaskTypicalWallTimeHours)
            && Objects.equals(subtasksPerCore, other.subtasksPerCore)
            && wallTimeScaling == other.wallTimeScaling;
    }
}
