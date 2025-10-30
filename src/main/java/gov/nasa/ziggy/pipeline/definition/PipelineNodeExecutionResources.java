package gov.nasa.ziggy.pipeline.definition;

import java.util.Objects;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import gov.nasa.ziggy.pipeline.step.remote.Architecture;
import gov.nasa.ziggy.pipeline.step.remote.BatchQueue;
import gov.nasa.ziggy.pipeline.step.remote.RemoteArchitectureOptimizer;
import gov.nasa.ziggy.pipeline.step.remote.RemoteEnvironment;
import gov.nasa.ziggy.worker.WorkerResources;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToOne;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

/**
 * Parameters relevant for configuring execution of a {@link PipelineNode}. The configuration is
 * related to a specific {@link PipelineNode} via fields that contain the node's pipeline step name
 * and pipeline name. This ensures that a given {@link PipelineNodeExecutionResources} is associated
 * with any and all versions of its node and that none of these parameters are involved in
 * determining whether a node definition is up to date (which would be the case if the class was
 * embedded).
 *
 * @author PT
 */

@Entity
@Table(name = "ziggy_PipelineNode_executionResources",
    uniqueConstraints = { @UniqueConstraint(columnNames = { "pipelineName", "pipelineStepName" }) })
public class PipelineNodeExecutionResources {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE,
        generator = "ziggy_PipelineNode_executionResources_generator")
    @SequenceGenerator(name = "ziggy_PipelineNode_executionResources_generator", initialValue = 1,
        sequenceName = "ziggy_PipelineNode_executionResources_sequence", allocationSize = 1)
    private Long id;

    // Fields that provide mapping to a specific pipeline.
    private final String pipelineName;
    private final String pipelineStepName;

    // Fields that control worker-side execution resource options.
    @OneToOne(cascade = CascadeType.ALL)
    private WorkerResources workerResources = new WorkerResources(0, 0F);

    private int maxFailedSubtaskCount = 0;
    private int maxAutoResubmits = 0;

    // Fields that control remote execution and are mandatory.
    private boolean remoteExecutionEnabled = false;
    private double subtaskMaxWallTimeHours = 0;
    private double subtaskTypicalWallTimeHours = 0;
    private double subtaskRamGigabytes = 0;
    @Enumerated(EnumType.STRING)
    private RemoteArchitectureOptimizer optimizer = RemoteArchitectureOptimizer.COST;
    private boolean nodeSharing = true;
    private boolean wallTimeScaling = true;

    // Optional parameters for remote execution. The user can specify these, or can
    // allow the remote execution calculator decide the values. For the Strings, an
    // empty string indicates that the user has not set a value. For the int and double
    // primitives, this is indicated by a zero; the exception is minSubtasksForRemoteExecution,
    // in which -1 indicates that the user has not entered a value.

    @ManyToOne
    private RemoteEnvironment remoteEnvironment;

    @ManyToOne
    private Architecture architecture;

    @ManyToOne
    private BatchQueue batchQueue;

    private String remoteNodeArchitecture = "";
    private String reservedQueueName = "";
    private int maxNodes;
    private double subtasksPerCore;
    private int minSubtasksForRemoteExecution = -1;

    // "The JPA specification requires that all persistent classes have a no-arg constructor. This
    // constructor may be public or protected."
    protected PipelineNodeExecutionResources() {
        this("", "");
    }

    public PipelineNodeExecutionResources(String pipelineName, String pipelineStepName) {
        this.pipelineName = pipelineName;
        this.pipelineStepName = pipelineStepName;
    }

    /** Copy constructor. */
    public PipelineNodeExecutionResources(PipelineNodeExecutionResources original) {
        this(original.pipelineName, original.pipelineStepName);
        populateFrom(original);
    }

    /**
     * Populates one instance with the values of another. This is useful for quickly putting values
     * from a copied instance back into the original. This allows the original to be merged back
     * into the database.
     */
    public void populateFrom(PipelineNodeExecutionResources other) {
        workerResources.setMaxWorkerCount(other.workerResources.getMaxWorkerCount());
        workerResources.setHeapSizeGigabytes(other.workerResources.getHeapSizeGigabytes());
        maxFailedSubtaskCount = other.maxFailedSubtaskCount;
        maxAutoResubmits = other.maxAutoResubmits;
        remoteExecutionEnabled = other.remoteExecutionEnabled;
        subtaskMaxWallTimeHours = other.subtaskMaxWallTimeHours;
        subtaskTypicalWallTimeHours = other.subtaskTypicalWallTimeHours;
        subtaskRamGigabytes = other.subtaskRamGigabytes;
        minSubtasksForRemoteExecution = other.minSubtasksForRemoteExecution;
        optimizer = other.optimizer;
        nodeSharing = other.nodeSharing;
        wallTimeScaling = other.wallTimeScaling;

        remoteNodeArchitecture = other.remoteNodeArchitecture;
        reservedQueueName = other.reservedQueueName;
        maxNodes = other.maxNodes;
        subtasksPerCore = other.subtasksPerCore;
        architecture = other.architecture;
        batchQueue = other.batchQueue;
        reservedQueueName = other.reservedQueueName;
        remoteEnvironment = other.remoteEnvironment;
    }

    /**
     * Returns the worker resources for the current node. If resources are not specified, the
     * resources object will have nulls, in which case default values will be retrieved from the
     * {@link WorkerResources} singleton when the object is queried.
     */
    public WorkerResources workerResources() {
        return workerResources;
    }

    /**
     * Applies the worker resources values to a pipeline node. If the values are the default ones,
     * the node's values will be set to zero rather than the values returned by the resources
     * object.
     */
    public void applyWorkerResources(WorkerResources resources) {
        workerResources.setMaxWorkerCount(resources.getMaxWorkerCount());
        workerResources.setHeapSizeGigabytes(resources.getHeapSizeGigabytes());
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

    public String getPipelineStepName() {
        return pipelineStepName;
    }

    public int getMaxWorkerCount() {
        return workerResources.getMaxWorkerCount();
    }

    public void setMaxWorkerCount(int maxWorkerCount) {
        workerResources.setMaxWorkerCount(maxWorkerCount);
    }

    public float getHeapSizeGigabytes() {
        return workerResources.getHeapSizeGigabytes();
    }

    public void setHeapSizeGigabytes(int heapSizeGigabytes) {
        workerResources.setHeapSizeGigabytes(heapSizeGigabytes);
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

    public double subtaskRamGigabytes() {
        return subtaskRamGigabytes;
    }

    public void setSubtaskRamGigabytes(double subtaskRamGigabytes) {
        this.subtaskRamGigabytes = subtaskRamGigabytes;
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

    public RemoteEnvironment getRemoteEnvironment() {
        return remoteEnvironment;
    }

    public void setRemoteEnvironment(RemoteEnvironment remoteEnvironment) {
        this.remoteEnvironment = remoteEnvironment;
    }

    public Architecture getArchitecture() {
        return architecture;
    }

    public void setArchitecture(Architecture architecture) {
        this.architecture = architecture;
    }

    public BatchQueue getBatchQueue() {
        return batchQueue;
    }

    public void setBatchQueue(BatchQueue batchQueue) {
        this.batchQueue = batchQueue;
    }

    public String getRemoteNodeArchitecture() {
        return remoteNodeArchitecture;
    }

    public void setRemoteNodeArchitecture(String remoteNodeArchitecture) {
        this.remoteNodeArchitecture = remoteNodeArchitecture;
    }

    public String getReservedQueueName() {
        return reservedQueueName;
    }

    public void setReservedQueueName(String queueName) {
        reservedQueueName = queueName;
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

    @Override
    public int hashCode() {
        return Objects.hash(architecture, batchQueue, subtaskRamGigabytes, workerResources,
            maxAutoResubmits, maxFailedSubtaskCount, maxNodes, minSubtasksForRemoteExecution,
            nodeSharing, optimizer, pipelineStepName, pipelineName, remoteEnvironment,
            remoteExecutionEnabled, remoteNodeArchitecture, reservedQueueName,
            subtaskMaxWallTimeHours, subtaskTypicalWallTimeHours, subtasksPerCore, wallTimeScaling);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PipelineNodeExecutionResources other = (PipelineNodeExecutionResources) obj;
        return Objects.equals(architecture, other.architecture)
            && Objects.equals(batchQueue, other.batchQueue)
            && Double.doubleToLongBits(subtaskRamGigabytes) == Double
                .doubleToLongBits(other.subtaskRamGigabytes)
            && workerResources.getHeapSizeGigabytes() == other.workerResources
                .getHeapSizeGigabytes()
            && maxAutoResubmits == other.maxAutoResubmits
            && maxFailedSubtaskCount == other.maxFailedSubtaskCount && maxNodes == other.maxNodes
            && workerResources.getMaxWorkerCount() == other.workerResources.getMaxWorkerCount()
            && minSubtasksForRemoteExecution == other.minSubtasksForRemoteExecution
            && nodeSharing == other.nodeSharing && optimizer == other.optimizer
            && Objects.equals(pipelineStepName, other.pipelineStepName)
            && Objects.equals(pipelineName, other.pipelineName)
            && Objects.equals(remoteEnvironment, other.remoteEnvironment)
            && remoteExecutionEnabled == other.remoteExecutionEnabled
            && Objects.equals(remoteNodeArchitecture, other.remoteNodeArchitecture)
            && Objects.equals(reservedQueueName, other.reservedQueueName)
            && Double.doubleToLongBits(subtaskMaxWallTimeHours) == Double
                .doubleToLongBits(other.subtaskMaxWallTimeHours)
            && Double.doubleToLongBits(subtaskTypicalWallTimeHours) == Double
                .doubleToLongBits(other.subtaskTypicalWallTimeHours)
            && Double.doubleToLongBits(subtasksPerCore) == Double
                .doubleToLongBits(other.subtasksPerCore)
            && wallTimeScaling == other.wallTimeScaling;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }
}
