package gov.nasa.ziggy.pipeline.definition;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.step.PipelineStep;
import gov.nasa.ziggy.worker.WorkerResources;
import jakarta.persistence.CascadeType;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OneToOne;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;

/**
 * Created by the {@link PipelineExecutor} when creating {@link PipelineTask} objects for a
 * {@link PipelineNode}
 * <p>
 * Associated with a {@link PipelineInstance}, and a {@link PipelineNode}. Contains the number of
 * {@link PipelineTask} objects created for this node and their aggregate states so that the
 * transition logic can properly determine whether all tasks for this node have completed.
 *
 * @author Todd Klaus
 */
@Entity
@Table(name = "ziggy_PipelineInstanceNode")
public class PipelineInstanceNode {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE,
        generator = "ziggy_PipelineInstanceNode_generator")
    @SequenceGenerator(name = "ziggy_PipelineInstanceNode_generator", initialValue = 1,
        sequenceName = "ziggy_PipelineInstanceNode_sequence", allocationSize = 1)
    private Long id;

    /** Timestamp this was created (either by launcher or transition logic) */
    private Date created = new Date();

    /**
     * Shortcut to the pipeline step name, so we don't always have to do a database access for it.
     */
    private String pipelineStepName;
    private String executableName;

    @ManyToMany
    @JoinTable(name = "ziggy_PipelineInstanceNode_parameterSets")
    private Set<ParameterSet> parameterSets = new HashSet<>();

    /** Indicates whether the node completed and kicked off the transition to the next node. */
    private boolean transitionComplete;

    /** Indicates whether the transition to the next instance node failed. */
    private boolean transitionFailed;

    @ElementCollection
    @JoinTable(name = "ziggy_PipelineInstanceNode_nextNodes")
    private List<PipelineInstanceNode> nextNodes = new ArrayList<>();

    @OneToMany
    @JoinTable(name = "ziggy_PipelineInstanceNode_pipelineTasks")
    private List<PipelineTask> pipelineTasks = new ArrayList<>();

    @ManyToOne
    private PipelineNode pipelineNode;

    @ManyToOne
    private PipelineStep pipelineStep;

    @OneToOne(cascade = CascadeType.ALL)
    private WorkerResources workerResources = new WorkerResources();

    /**
     * Required by Hibernate
     */
    public PipelineInstanceNode() {
    }

    public PipelineInstanceNode(PipelineNode pipelineNode, PipelineStep pipelineStep) {
        this.pipelineNode = pipelineNode;
        setPipelineStep(pipelineStep);
        executableName = pipelineStep.getFile();
    }

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public String getPipelineStepName() {
        return pipelineStepName;
    }

    public String getExecutableName() {
        return executableName;
    }

    public Long getId() {
        return id;
    }

    public boolean isPersistedToDatabase() {
        return id != null && id.longValue() > 0;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

    public Set<ParameterSet> getParameterSets() {
        return parameterSets;
    }

    public void setParameterSets(Set<ParameterSet> parameterSets) {
        this.parameterSets = parameterSets;
    }

    public boolean isTransitionComplete() {
        return transitionComplete;
    }

    public void setTransitionComplete(boolean transitionComplete) {
        this.transitionComplete = transitionComplete;
    }

    public boolean isTransitionFailed() {
        return transitionFailed;
    }

    public void setTransitionFailed(boolean transitionFailed) {
        this.transitionFailed = transitionFailed;
    }

    public List<PipelineInstanceNode> getNextNodes() {
        return nextNodes;
    }

    public void setNextNodes(List<PipelineInstanceNode> nextNodes) {
        this.nextNodes = nextNodes;
    }

    public List<PipelineTask> getPipelineTasks() {
        return pipelineTasks;
    }

    public void setPipelineTasks(List<PipelineTask> pipelineTasks) {
        this.pipelineTasks = pipelineTasks;
    }

    public void addPipelineTask(PipelineTask pipelineTask) {
        pipelineTasks.add(pipelineTask);
    }

    public PipelineNode getPipelineNode() {
        return pipelineNode;
    }

    public void setPipelineNode(PipelineNode pipelineNode) {
        this.pipelineNode = pipelineNode;
    }

    public PipelineStep getPipelineStep() {
        return pipelineStep;
    }

    public void setPipelineStep(PipelineStep pipelineStep) {
        this.pipelineStep = pipelineStep;
        pipelineStepName = pipelineStep.getName();
    }

    public WorkerResources getWorkerResources() {
        return workerResources;
    }

    public void setMaxWorkerCount(int maxWorkerCount) {
        workerResources.setMaxWorkerCount(maxWorkerCount);
    }

    public void setHeapSizeGigabytes(float heapSizeGigabytes) {
        workerResources.setHeapSizeGigabytes(heapSizeGigabytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PipelineInstanceNode other = (PipelineInstanceNode) obj;
        return Objects.equals(id, other.id);
    }
}
