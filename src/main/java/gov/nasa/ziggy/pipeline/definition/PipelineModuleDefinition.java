package gov.nasa.ziggy.pipeline.definition;

import java.util.Objects;

import gov.nasa.ziggy.module.DatastoreDirectoryPipelineInputs;
import gov.nasa.ziggy.module.DatastoreDirectoryPipelineOutputs;
import gov.nasa.ziggy.module.ExternalProcessPipelineModule;
import gov.nasa.ziggy.module.PipelineInputs;
import gov.nasa.ziggy.module.PipelineOutputs;
import gov.nasa.ziggy.uow.DatastoreDirectoryUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;
import jakarta.persistence.AttributeOverride;
import jakarta.persistence.AttributeOverrides;
import jakarta.persistence.Column;
import jakarta.persistence.Embedded;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import jakarta.persistence.UniqueConstraint;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

/**
 * This class models a pipeline module, which consists of an algorithm and the parameters that
 * control the behavior of that algorithm.
 * <p>
 * By default, pipeline module definitions will use{@link ExternalProcessPipelineModule} for their
 * execution module, {@link DatastoreDirectoryUnitOfWorkGenerator} to generate units of work,
 * {@link DatastoreDirectoryPipelineInputs} and {@link DatastoreDirectoryPipelineOutputs},
 * respectively, for the inputs and outputs class. In the case where the user wishes to accept these
 * defaults, there is no need to specify any of them in the module definition.
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

    @XmlAttribute
    private String description = "description";

    @XmlAttribute(required = false, name = "uowGenerator")
    @XmlJavaTypeAdapter(ClassWrapper.ClassWrapperAdapter.class)
    @Embedded
    @AttributeOverrides({
        @AttributeOverride(name = "clazz", column = @Column(name = "unitOfWorkGenerator")) })
    private ClassWrapper<UnitOfWorkGenerator> unitOfWorkGenerator;

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
        DatastoreDirectoryPipelineInputs.class);

    @Embedded
    @XmlAttribute(required = false)
    @XmlJavaTypeAdapter(ClassWrapper.ClassWrapperAdapter.class)
    @AttributeOverrides({
        @AttributeOverride(name = "clazz", column = @Column(name = "outputsClass")) })
    private ClassWrapper<PipelineOutputs> outputsClass = new ClassWrapper<>(
        DatastoreDirectoryPipelineOutputs.class);

    // Using the Integer class rather than int here because XML won't allow optional
    // attributes that are primitive types. Transient so that modules can be imported
    // with the value set, but the value can then get put into the database in an
    // instance of PipelineModuleExecutionResources.
    @Transient
    @XmlAttribute(required = false)
    private Integer exeTimeoutSecs = PipelineModuleExecutionResources.DEFAULT_TIMEOUT_SECONDS;

    // Using the Integer class rather than int here because XML won't allow optional
    // attributes that are primitive types
    @Transient
    @XmlAttribute(required = false)
    private Integer minMemoryMegabytes = PipelineModuleExecutionResources.DEFAULT_MEMORY_MEGABYTES;

    // for hibernate use only
    public PipelineModuleDefinition() {
    }

    public PipelineModuleDefinition(String name) {
        setName(name);
    }

    /**
     * @return Returns the id.
     */
    public Long getId() {
        return id;
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

    public int getMinMemoryMegabytes() {
        return minMemoryMegabytes;
    }

    public ClassWrapper<UnitOfWorkGenerator> getUnitOfWorkGenerator() {
        return unitOfWorkGenerator;
    }

    public void setUnitOfWorkGenerator(ClassWrapper<UnitOfWorkGenerator> unitOfWorkGenerator) {
        this.unitOfWorkGenerator = unitOfWorkGenerator;
    }

    @Override
    public int hashCode() {
        return Objects.hash(description, getOptimisticLockValue(), id, inputsClass, isLocked(),
            getName(), outputsClass, pipelineModuleClass, getVersion());
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
        equalModule = equalModule && Objects.equals(id, other.id);
        equalModule = equalModule && Objects.equals(inputsClass, other.inputsClass);
        equalModule = equalModule && isLocked() == other.isLocked();
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
