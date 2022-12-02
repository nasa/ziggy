package gov.nasa.ziggy.pipeline.definition;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.crud.HasExternalIdCrud;

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
@Table(name = "PI_PIPELINE_INSTANCE")
public class PipelineInstance implements PipelineExecutionTime {
    private static final Logger log = LoggerFactory.getLogger(PipelineInstance.class);

    public static final int HIGHEST_PRIORITY = 0;
    public static final int LOWEST_PRIORITY = 4;

    public enum State {
        /** Not yet launched */
        INITIALIZED,

        /** pipeline running or ready to run, no failed tasks */
        PROCESSING,

        /** at least one failed task, but others still running or ready to run */
        ERRORS_RUNNING,

        /** at least one failed task, no tasks can run (pipeline stalled) */
        ERRORS_STALLED,

        /** pipeline stopped/paused by operator */
        STOPPED,

        /** all tasks completed successfully */
        COMPLETED
    }

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "sg")
    @SequenceGenerator(name = "sg", initialValue = 1, sequenceName = "PI_PIPE_INST_SEQ",
        allocationSize = 1)
    private long id;

    /**
     * Descriptive name specified by the user at launch-time. Used when displaying the instance in
     * the console. Does not have to be unique
     */
    private String name;

    @ManyToOne
    private Group group = null;

    /** Timestamp that processing started on this pipeline instance */
    private Date startProcessingTime = new Date(0);

    /** Timestamp that processing ended (successfully) on this pipeline instance */
    private Date endProcessingTime = new Date(0);

    private long currentExecutionStartTimeMillis = -1;

    private long priorProcessingExecutionTimeMillis;

    @ManyToOne
    private PipelineDefinition pipelineDefinition = null;

    @Enumerated(EnumType.STRING)
    private State state = State.INITIALIZED;
    private int priority = LOWEST_PRIORITY;

    /**
     * {@link ParameterSet}s used as {@link Parameters} for this instance. This is a hard-reference
     * to a specific version of the {@link ParameterSet}, selected at launch time (typically the
     * latest available version)
     */
    @ManyToMany
    @JoinTable(name = "PI_INSTANCE_PS")
    private Map<ClassWrapper<Parameters>, ParameterSet> pipelineParameterSets = new HashMap<>();

    /**
     * {@link ModelRegistry} in force at the time this pipeline instance was launched. For data
     * accountability
     */
    @ManyToOne
    private ModelRegistry modelRegistry = null;

    /**
     * Name of the Trigger that launched this pipeline
     */
    private String triggerName;

    /** If set, the pipeline instance will start at the specified node */
    @OneToOne
    @JoinColumn(name = "START_NODE")
    private PipelineInstanceNode startNode;

    /**
     * If set, the pipeline instance will end at the specified node. Note that only one endNode can
     * be specified, so only one branch can be terminated early. If the pipeline branches before
     * this endNode, other branches will proceed to the end.
     */
    @OneToOne
    @JoinColumn(name = "END_NODE")
    private PipelineInstanceNode endNode;

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

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public PipelineDefinition getPipelineDefinition() {
        return pipelineDefinition;
    }

    public void setPipelineDefinition(PipelineDefinition pipeline) {
        pipelineDefinition = pipeline;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
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

    /**
     * @return the triggerName
     */
    public String getTriggerName() {
        return triggerName;
    }

    /**
     * @param triggerName the triggerName to set
     */
    public void setTriggerName(String triggerName) {
        this.triggerName = triggerName;
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
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final PipelineInstance other = (PipelineInstance) obj;
        if (id != other.id) {
            return false;
        }
        return true;
    }

    public Map<ClassWrapper<Parameters>, ParameterSet> getPipelineParameterSets() {
        return pipelineParameterSets;
    }

    public void setPipelineParameterSets(
        Map<ClassWrapper<Parameters>, ParameterSet> pipelineParameterSets) {
        this.pipelineParameterSets = pipelineParameterSets;
        populateXmlFields();
    }

    public void populateXmlFields() {
        for (ParameterSet parameterSet : pipelineParameterSets.values()) {
            parameterSet.populateXmlFields();
        }
    }

    public boolean hasPipelineParameters(Class<? extends Parameters> parametersClass) {
        ClassWrapper<Parameters> classWrapper = new ClassWrapper<>(parametersClass);

        return pipelineParameterSets.get(classWrapper) != null;
    }

    /**
     * Convenience method for getting the pipeline parameters for the specified {@link Parameters}
     * class for this {@link PipelineInstance}
     *
     * @param parametersClass
     * @return
     */
    public <T extends Parameters> T getPipelineParameters(Class<T> parametersClass) {
        ClassWrapper<Parameters> classWrapper = new ClassWrapper<>(parametersClass);
        ParameterSet pipelineParamSet = pipelineParameterSets.get(classWrapper);

        if (pipelineParamSet != null) {
            log.debug("Pipeline parameters for class: " + parametersClass + " found");
            return pipelineParamSet.parametersInstance();
        } else {
            throw new PipelineException("Pipeline parameters for class: " + parametersClass
                + " not found in PipelineInstance");
        }
    }

    /**
     * Retrieve module {@link Parameters} for this {@link PipelineInstance}. This method is not
     * intended to be called directly, use the convenience method
     * {@link PipelineTask}.getParameters().
     *
     * @param parametersClass
     * @return
     */
    ParameterSet getPipelineParameterSet(Class<? extends Parameters> parametersClass) {
        ClassWrapper<Parameters> classWrapper = new ClassWrapper<>(parametersClass);
        return pipelineParameterSets.get(classWrapper);
    }

    public void clearParameterSets() {
        pipelineParameterSets.clear();
    }

    public ParameterSet putParameterSet(ClassWrapper<Parameters> key, ParameterSet value) {
        value.populateXmlFields();
        return pipelineParameterSets.put(key, value);
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the startNode
     */
    public PipelineInstanceNode getStartNode() {
        return startNode;
    }

    /**
     * @param startNode the startNode to set
     */
    public void setStartNode(PipelineInstanceNode startNode) {
        this.startNode = startNode;
    }

    /**
     * @return the endNode
     */
    public PipelineInstanceNode getEndNode() {
        return endNode;
    }

    /**
     * @param endNode the endNode to set
     */
    public void setEndNode(PipelineInstanceNode endNode) {
        this.endNode = endNode;
    }

    public Group getGroup() {
        return group;
    }

    public void setGroup(Group group) {
        this.group = group;
    }

    /**
     * @return the modelRegistry
     */
    public ModelRegistry getModelRegistry() {
        return modelRegistry;
    }

    /**
     * @param modelRegistry the modelRegistry to set
     */
    public void setModelRegistry(ModelRegistry modelRegistry) {
        this.modelRegistry = modelRegistry;
    }

    public <T extends HasExternalId> T retrieveModel(HasExternalIdCrud<T> modelCrud,
        String modelType) {
        if (modelCrud == null) {
            throw new IllegalArgumentException("modelCrud cannot be null.");
        }

        ModelRegistry modelRegistry = getModelRegistry();
        ModelMetadata modelMetadata = modelRegistry.getMetadataForType(modelType);
        int dataSetId = Integer.parseInt(modelMetadata.getModelRevision());
        T model = modelCrud.retrieveByExternalId(dataSetId);

        return model;
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
