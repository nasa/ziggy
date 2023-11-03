package gov.nasa.ziggy.pipeline.definition;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import gov.nasa.ziggy.module.DefaultPipelineInputs;
import gov.nasa.ziggy.module.DefaultPipelineOutputs;
import gov.nasa.ziggy.module.ExternalProcessPipelineModule;
import gov.nasa.ziggy.module.PipelineInputs;
import gov.nasa.ziggy.module.PipelineOutputs;
import gov.nasa.ziggy.parameters.ParametersInterface;
import jakarta.persistence.AttributeOverride;
import jakarta.persistence.AttributeOverrides;
import jakarta.persistence.Column;
import jakarta.persistence.Embedded;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

/**
 * This class models a pipeline module, which consists of an algorithm and the parameters that
 * control the behavior of that algorithm.
 *
 * @author Todd Klaus
 */
@XmlAccessorType(XmlAccessType.NONE)
@Entity
@Table(name = "ziggy_PipelineModuleDefinition",
    uniqueConstraints = { @UniqueConstraint(columnNames = { "name", "version" }) })
public class PipelineModuleDefinition
    extends UniqueNameVersionPipelineComponent<PipelineModuleDefinition> {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE,
        generator = "ziggy_PipelineModuleDefinition_generator")
    @SequenceGenerator(name = "ziggy_PipelineModuleDefinition_generator", initialValue = 1,
        sequenceName = "ziggy_PipelineModuleDefinition_sequence", allocationSize = 1)
    private Long id;

    @ManyToOne
    private Group group = null;

    @Embedded
    // init with empty placeholder, to be filled in by console
    private AuditInfo auditInfo = new AuditInfo();

    @XmlAttribute
    private String description = "description";

    @XmlAttribute(required = false)
    @XmlJavaTypeAdapter(ClassWrapper.ClassWrapperAdapter.class)
    @Embedded
    @AttributeOverrides({
        @AttributeOverride(name = "clazz", column = @Column(name = "moduleClass")) })
    private ClassWrapper<PipelineModule> pipelineModuleClass = new ClassWrapper<>(
        ExternalProcessPipelineModule.class);

    @Embedded
    @XmlAttribute(required = false)
    @XmlJavaTypeAdapter(ClassWrapper.ClassWrapperAdapter.class)
    @AttributeOverrides({
        @AttributeOverride(name = "clazz", column = @Column(name = "inputsClass")) })
    private ClassWrapper<PipelineInputs> inputsClass = new ClassWrapper<>(
        DefaultPipelineInputs.class);

    @Embedded
    @XmlAttribute(required = false)
    @XmlJavaTypeAdapter(ClassWrapper.ClassWrapperAdapter.class)
    @AttributeOverrides({
        @AttributeOverride(name = "clazz", column = @Column(name = "outputsClass")) })
    private ClassWrapper<PipelineOutputs> outputsClass = new ClassWrapper<>(
        DefaultPipelineOutputs.class);

    // Using the Integer class rather than int here because XML won't allow optional
    // attributes that are primitive types
    @XmlAttribute(required = false)
    private Integer exeTimeoutSecs = 60 * 60 * 50; // 50 hours

    // Using the Integer class rather than int here because XML won't allow optional
    // attributes that are primitive types
    @XmlAttribute(required = false)
    private Integer minMemoryMegaBytes = 0; // zero means memory usage is not constrained

    // for hibernate use only
    public PipelineModuleDefinition() {
    }

    public PipelineModuleDefinition(String name) {
        setName(name);
    }

    public PipelineModuleDefinition(AuditInfo auditInfo, String name) {
        this.auditInfo = auditInfo;
        setName(name);
    }

    /**
     * @return Returns the id.
     */
    public Long getId() {
        return id;
    }

    public AuditInfo getAuditInfo() {
        return auditInfo;
    }

    public void setAuditInfo(AuditInfo auditInfo) {
        this.auditInfo = auditInfo;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int getExeTimeoutSecs() {
        return exeTimeoutSecs;
    }

    public void setExeTimeoutSecs(int exeTimeoutSecs) {
        this.exeTimeoutSecs = exeTimeoutSecs;
    }

    public ClassWrapper<PipelineModule> getPipelineModuleClass() {
        return pipelineModuleClass;
    }

    public void setPipelineModuleClass(ClassWrapper<PipelineModule> pipelineModuleClass) {
        this.pipelineModuleClass = pipelineModuleClass;
    }

    public ClassWrapper<PipelineInputs> getInputsClass() {
        return inputsClass;
    }

    public void setInputsClass(ClassWrapper<PipelineInputs> inputsClass) {
        this.inputsClass = inputsClass;
    }

    public ClassWrapper<PipelineOutputs> getOutputsClass() {
        return outputsClass;
    }

    public void setOutputsClass(ClassWrapper<PipelineOutputs> outputsClass) {
        this.outputsClass = outputsClass;
    }

    /**
     * @return the minMemoryBytes
     */
    public int getMinMemoryMegaBytes() {
        return minMemoryMegaBytes;
    }

    /**
     * @param minMemoryBytes the minMemoryBytes to set
     */
    public void setMinMemoryMegaBytes(int minMemoryBytes) {
        minMemoryMegaBytes = minMemoryBytes;
    }

    public Set<ClassWrapper<ParametersInterface>> getRequiredParameterClasses() {
        PipelineInputs pipelineInputs = inputsClass.newInstance();

        Set<ClassWrapper<ParametersInterface>> requiredParameters = new HashSet<>();
        List<Class<? extends ParametersInterface>> moduleParameters = pipelineInputs
            .requiredParameters();

        for (Class<? extends ParametersInterface> clazz : moduleParameters) {
            requiredParameters.add(new ClassWrapper<>(clazz));
        }

        return requiredParameters;
    }

    public Group getGroup() {
        return group;
    }

    public void setGroup(Group group) {
        this.group = group;
    }

    @Override
    public int hashCode() {
        return Objects.hash(description, getOptimisticLockValue(), exeTimeoutSecs, group, id,
            inputsClass, isLocked(), minMemoryMegaBytes, getName(), outputsClass,
            pipelineModuleClass, getVersion());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PipelineModuleDefinition other = (PipelineModuleDefinition) obj;
        boolean equalModule = Objects.equals(description, other.description);
        equalModule = equalModule && getOptimisticLockValue() == other.getOptimisticLockValue();
        equalModule = equalModule && exeTimeoutSecs.intValue() == other.exeTimeoutSecs.intValue();
        equalModule = equalModule && Objects.equals(group, other.group);
        equalModule = equalModule && Objects.equals(id, other.id);
        equalModule = equalModule && Objects.equals(inputsClass, other.inputsClass);
        equalModule = equalModule && isLocked() == other.isLocked();
        equalModule = equalModule
            && minMemoryMegaBytes.intValue() == other.minMemoryMegaBytes.intValue();
        equalModule = equalModule && Objects.equals(getName(), other.getName());
        equalModule = equalModule && Objects.equals(outputsClass, other.outputsClass);
        equalModule = equalModule && Objects.equals(pipelineModuleClass, other.pipelineModuleClass);
        return equalModule && getVersion() == other.getVersion();
    }

    @Override
    protected void clearDatabaseId() {
        id = null;
    }

    @Override
    public boolean totalEquals(Object obj) {
        return equals(obj);
    }
}
