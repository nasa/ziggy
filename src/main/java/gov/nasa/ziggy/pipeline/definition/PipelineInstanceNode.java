package gov.nasa.ziggy.pipeline.definition;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;

/**
 * Created by the {@link PipelineExecutor} when creating {@link PipelineTask} objects for a
 * {@link PipelineDefinitionNode}
 * <p>
 * Associated with a {@link PipelineInstance}, and a {@link PipelineDefinitionNode}. Contains the
 * number of {@link PipelineTask} objects created for this node and their aggregate states so that
 * the transition logic can properly determine whether all tasks for this node have completed.
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

    @ManyToOne
    private PipelineInstance pipelineInstance;

    @ManyToOne
    private PipelineDefinitionNode pipelineDefinitionNode;

    @ManyToOne
    private PipelineModuleDefinition pipelineModuleDefinition;

    /**
     * {@link ParameterSet}s used as {@link Parameters} for this instance. This is a hard-reference
     * to a specific version of the {@link ParameterSet}, selected at launch time (typically the
     * latest available version)
     */
    @ManyToMany
    @JoinTable(name = "ziggy_PipelineInstanceNode_moduleParameterSets")
    private Map<ClassWrapper<ParametersInterface>, ParameterSet> moduleParameterSets = new HashMap<>();

    /** Indicates whether the node completed and kicked off the transition to the next node. */
    private boolean transitionComplete;

    /**
     * Required by Hibernate
     */
    public PipelineInstanceNode() {
    }

    public PipelineInstanceNode(PipelineInstance pipelineInstance,
        PipelineDefinitionNode pipelineDefinitionNode,
        PipelineModuleDefinition pipelineModuleDefinition) {
        this.pipelineInstance = pipelineInstance;
        this.pipelineDefinitionNode = pipelineDefinitionNode;
        this.pipelineModuleDefinition = pipelineModuleDefinition;
    }

    public PipelineDefinitionNode getPipelineDefinitionNode() {
        return pipelineDefinitionNode;
    }

    public void setPipelineDefinitionNode(PipelineDefinitionNode configNode) {
        pipelineDefinitionNode = configNode;
    }

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public PipelineInstance getPipelineInstance() {
        return pipelineInstance;
    }

    public void setPipelineInstance(PipelineInstance instance) {
        pipelineInstance = instance;
    }

    public Long getId() {
        return id;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

    public PipelineModuleDefinition getPipelineModuleDefinition() {
        return pipelineModuleDefinition;
    }

    public void setPipelineModuleDefinition(PipelineModuleDefinition pipelineModuleDefinition) {
        this.pipelineModuleDefinition = pipelineModuleDefinition;
    }

    /**
     * Retrieve module {@link Parameters} for this {@link PipelineInstanceNode}. This method is not
     * intended to be called directly, use the convenience method
     * {@link PipelineTask}.getParameters().
     *
     * @param parametersClass
     * @return
     */
    ParameterSet getModuleParameterSet(Class<? extends Parameters> parametersClass) {
        ClassWrapper<Parameters> classWrapper = new ClassWrapper<>(parametersClass);
        return moduleParameterSets.get(classWrapper);
    }

    public Map<ClassWrapper<ParametersInterface>, ParameterSet> getModuleParameterSets() {
        return moduleParameterSets;
    }

    public void setModuleParameterSets(
        Map<ClassWrapper<ParametersInterface>, ParameterSet> moduleParameterSets) {
        this.moduleParameterSets = moduleParameterSets;
        populateXmlFields();
    }

    public boolean isTransitionComplete() {
        return transitionComplete;
    }

    public void setTransitionComplete(boolean transitionComplete) {
        this.transitionComplete = transitionComplete;
    }

    public void populateXmlFields() {
        for (ParameterSet parameterSet : moduleParameterSets.values()) {
            parameterSet.populateXmlFields();
        }
        pipelineInstance.populateXmlFields();
    }

    public ParameterSet putModuleParameterSet(Class<ParametersInterface> clazz,
        ParameterSet paramSet) {
        paramSet.populateXmlFields();
        ClassWrapper<ParametersInterface> classWrapper = new ClassWrapper<>(clazz);
        return moduleParameterSets.put(classWrapper, paramSet);
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
