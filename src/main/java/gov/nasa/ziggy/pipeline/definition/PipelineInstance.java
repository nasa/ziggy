package gov.nasa.ziggy.pipeline.definition;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OneToOne;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import jakarta.xml.bind.annotation.adapters.XmlAdapter;

/**
 * Represents an instance of a {@link PipelineDefinition} that either is running or has completed.
 * An instance of this class is created when a {@link PipelineDefinition} is launched by a trigger.
 * <p>
 * Pipeline instances have a priority level which determines the JMS priority for the messages that
 * contain the individual unit of work tasks for this pipeline instance.
 * <p>
 * Note that the {@link #equals(Object)} and {@link #hashCode()} methods are written in terms of
 * just the {@code id} field so this object should not be used in sets and maps until it has been
 * stored in the database
 *
 * @author Todd Klaus
 */
@Entity
@Table(name = "ziggy_PipelineInstance")
public class PipelineInstance implements PipelineExecutionTime {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(PipelineInstance.class);

    public enum State {
        /** Not yet launched */
        INITIALIZED(PipelineInstance::startExecutionClock),

        /** pipeline running or ready to run, no failed tasks */
        PROCESSING(PipelineInstance::startExecutionClock),

        /** at least one failed task, but others still running or ready to run */
        ERRORS_RUNNING(PipelineInstance::startExecutionClock),

        /** at least one failed task, no tasks can run (pipeline stalled) */
        ERRORS_STALLED(PipelineInstance::stopExecutionClock),

        /** all tasks completed successfully */
        COMPLETED(PipelineInstance::stopExecutionClock);

        private final Consumer<PipelineInstance> setExecutionClockState;

        State(Consumer<PipelineInstance> setExecutionClockState) {
            this.setExecutionClockState = setExecutionClockState;
        }

        public void setExecutionClockState(PipelineInstance instance) {
            setExecutionClockState.accept(instance);
        }
    }

    /**
     * Execution priority of a pipeline and its modules.
     * <p>
     * Note: Java enumerations have a natural ordering that matches the order in the definition. The
     * {@link Priority} enum is used to determine the order with which tasks are moved from the
     * submitted state to execution, hence do not change the ordering of the enums in this
     * definition.
     *
     * @author PT
     */
    public enum Priority {
        HIGHEST, HIGH, NORMAL, LOW, LOWEST;

        public static class PriorityXmlAdapter extends XmlAdapter<String, Priority> {

            @Override
            public String marshal(Priority priority) throws Exception {
                return priority.name();
            }

            @Override
            public Priority unmarshal(String priority) throws Exception {
                return Priority.valueOf(priority);
            }
        }
    }

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE,
        generator = "ziggy_PipelineInstance_generator")
    @SequenceGenerator(name = "ziggy_PipelineInstance_generator", initialValue = 1,
        sequenceName = "ziggy_PipelineInstance_sequence", allocationSize = 1)
    private Long id;

    /**
     * Descriptive name specified by the user at launch-time. Used when displaying the instance in
     * the console. Does not have to be unique.
     */
    private String name;

    /** Timestamp that processing started on this pipeline instance */
    private Date startProcessingTime = new Date(0);

    /** Timestamp that processing ended (successfully) on this pipeline instance */
    private Date endProcessingTime = new Date(0);

    private long currentExecutionStartTimeMillis = -1;

    private long priorProcessingExecutionTimeMillis;

    @ManyToOne
    private PipelineDefinition pipelineDefinition;

    @Enumerated(EnumType.STRING)
    private State state = State.INITIALIZED;

    @Enumerated(EnumType.STRING)
    private Priority priority = Priority.NORMAL;

    /**
     * {@link ParameterSet}s used by this instance. This is a hard-reference to a specific version
     * of the {@link ParameterSet}, selected at launch time (typically the latest available version)
     */
    @ManyToMany
    @JoinTable(name = "Ziggy_PipelineInstance_parameterSets")
    private Set<ParameterSet> parameterSets = new HashSet<>();
    /**
     * {@link ModelRegistry} in force at the time this pipeline instance was launched. For data
     * accountability
     */
    @ManyToOne
    private ModelRegistry modelRegistry = null;

    /** If set, the pipeline instance will start at the specified node */
    @OneToOne
    @JoinColumn(name = "startNode")
    private PipelineInstanceNode startNode;

    /**
     * If set, the pipeline instance will end at the specified node. Note that only one endNode can
     * be specified, so only one branch can be terminated early. If the pipeline branches before
     * this endNode, other branches will proceed to the end.
     */
    @OneToOne
    @JoinColumn(name = "endNode")
    private PipelineInstanceNode endNode;

    @OneToMany
    @JoinTable(name = "ziggy_PipelineInstance_rootNodes")
    private List<PipelineInstanceNode> rootNodes = new ArrayList<>();

    @OneToMany
    @JoinTable(name = "ziggy_PipelineInstance_pipelineInstanceNodes")
    private List<PipelineInstanceNode> pipelineInstanceNodes = new ArrayList<>();

    /**
     * Required by Hibernate
     */
    public PipelineInstance() {
    }

    public PipelineInstance(PipelineDefinition pipeline) {
        pipelineDefinition = pipeline;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public PipelineDefinition getPipelineDefinition() {
        return pipelineDefinition;
    }

    public void setPipelineDefinition(PipelineDefinition pipeline) {
        pipelineDefinition = pipeline;
    }

    public Priority getPriority() {
        return priority;
    }

    public void setPriority(Priority priority) {
        this.priority = priority;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

    @Override
    public Date getEndProcessingTime() {
        return endProcessingTime;
    }

    @Override
    public void setEndProcessingTime(Date endProcessingTime) {
        this.endProcessingTime = endProcessingTime;
    }

    @Override
    public Date getStartProcessingTime() {
        return startProcessingTime;
    }

    @Override
    public void setStartProcessingTime(Date startProcessingTime) {
        this.startProcessingTime = startProcessingTime;
    }

    public List<PipelineInstanceNode> getRootNodes() {
        return rootNodes;
    }

    public void setRootNodes(List<PipelineInstanceNode> rootNodes) {
        this.rootNodes = rootNodes;
    }

    public void addRootNode(PipelineInstanceNode rootNode) {
        rootNodes.add(rootNode);
    }

    public List<PipelineInstanceNode> getPipelineInstanceNodes() {
        return pipelineInstanceNodes;
    }

    public void setPipelineInstanceNodes(List<PipelineInstanceNode> pipelineInstanceNodes) {
        this.pipelineInstanceNodes = pipelineInstanceNodes;
    }

    public void addPipelineInstanceNode(PipelineInstanceNode pipelineInstanceNode) {
        pipelineInstanceNodes.add(pipelineInstanceNode);
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
        PipelineInstance other = (PipelineInstance) obj;
        return Objects.equals(id, other.id);
    }

    public Set<ParameterSet> getParameterSets() {
        return parameterSets;
    }

    public void setParameterSets(Set<ParameterSet> parameterSets) {
        this.parameterSets = parameterSets;
    }

    public void addParameterSet(ParameterSet parameterSet) {
        parameterSets.add(parameterSet);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public PipelineInstanceNode getStartNode() {
        return startNode;
    }

    public void setStartNode(PipelineInstanceNode startNode) {
        this.startNode = startNode;
    }

    public PipelineInstanceNode getEndNode() {
        return endNode;
    }

    public void setEndNode(PipelineInstanceNode endNode) {
        this.endNode = endNode;
    }

    public ModelRegistry getModelRegistry() {
        return modelRegistry;
    }

    public void setModelRegistry(ModelRegistry modelRegistry) {
        this.modelRegistry = modelRegistry;
    }

    public String getModelDescription(String modelType) {
        ModelRegistry modelRegistry = getModelRegistry();
        ModelMetadata modelMetadata = modelRegistry.getMetadataForType(modelType);

        return modelMetadata.getModelDescription();
    }

    @Override
    public void setPriorProcessingExecutionTimeMillis(long priorProcessingExecutionTimeMillis) {
        this.priorProcessingExecutionTimeMillis = priorProcessingExecutionTimeMillis;
    }

    @Override
    public long getPriorProcessingExecutionTimeMillis() {
        return priorProcessingExecutionTimeMillis;
    }

    @Override
    public void setCurrentExecutionStartTimeMillis(long currentExecutionStartTimeMillis) {
        this.currentExecutionStartTimeMillis = currentExecutionStartTimeMillis;
    }

    @Override
    public long getCurrentExecutionStartTimeMillis() {
        return currentExecutionStartTimeMillis;
    }
}
