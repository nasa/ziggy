package gov.nasa.ziggy.pipeline.step;

import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.PipelineStepExecutor;
import gov.nasa.ziggy.pipeline.definition.UniqueNameVersionPipelineComponent;
import gov.nasa.ziggy.pipeline.definition.importer.PipelineDefinitionFile.PipelineDefinitionElement;
import gov.nasa.ziggy.pipeline.step.io.DatastoreDirectoryPipelineInputs;
import gov.nasa.ziggy.pipeline.step.io.DatastoreDirectoryPipelineOutputs;
import gov.nasa.ziggy.pipeline.step.io.PipelineInputs;
import gov.nasa.ziggy.pipeline.step.io.PipelineOutputs;
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
 * This class models a pipeline step, which consists of an algorithm and the parameters that control
 * the behavior of that algorithm.
 * <p>
 * By default, pipeline steps use {@link AlgorithmPipelineStepExecutor} to perform their execution
 * and {@link DatastoreDirectoryUnitOfWorkGenerator} to generate units of work. In addition,
 * {@link DatastoreDirectoryPipelineInputs} and {@link DatastoreDirectoryPipelineOutputs} are used
 * as the inputs and outputs classes respectively. In the case where the user wishes to accept these
 * defaults, there is no need to specify any of them in the step element.
 *
 * @author Todd Klaus
 */
@XmlAccessorType(XmlAccessType.NONE)
@Entity
@Table(name = "ziggy_PipelineStep",
    uniqueConstraints = { @UniqueConstraint(columnNames = { "name", "version" }) })
public class PipelineStep extends UniqueNameVersionPipelineComponent<PipelineStep>
    implements PipelineDefinitionElement {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "ziggy_PipelineStep_generator")
    @SequenceGenerator(name = "ziggy_PipelineStep_generator", initialValue = 1,
        sequenceName = "ziggy_PipelineStep_sequence", allocationSize = 1)
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
        @AttributeOverride(name = "clazz", column = @Column(name = "pipelineStepExecutorClass")) })
    private ClassWrapper<PipelineStepExecutor> pipelineStepExecutorClass = new ClassWrapper<>(
        AlgorithmPipelineStepExecutor.class);

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
    // attributes that are primitive types. Transient so that steps can be imported
    // with the value set, but the value can then get put into the database in an
    // instance of PipelineStepExecutionResources.
    @Transient
    @XmlAttribute(required = false)
    private Integer exeTimeoutSecs = PipelineStepExecutionResources.DEFAULT_TIMEOUT_SECONDS;

    // Using the Integer class rather than int here because XML won't allow optional
    // attributes that are primitive types
    @Transient
    @XmlAttribute(required = false)
    private Integer minMemoryMegabytes = PipelineStepExecutionResources.DEFAULT_MEMORY_MEGABYTES;

    // Executable file name, if different from step name.
    @XmlAttribute(required = false)
    private String file;

    // for hibernate use only
    public PipelineStep() {
    }

    public PipelineStep(String name) {
        setName(name);
    }

    /**
     * @return Returns the id.
     */
    @Override
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

    public ClassWrapper<PipelineStepExecutor> getPipelineStepExecutorClass() {
        return pipelineStepExecutorClass;
    }

    public void setPipelineStepExecutorClass(
        ClassWrapper<PipelineStepExecutor> pipelineStepExecutorClass) {
        this.pipelineStepExecutorClass = pipelineStepExecutorClass;
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

    /**
     * Returns the executable that must be run for this step. If the executable name is not
     * specified, the step name is returned. This allows the executable name to be optional, hence
     * used only in cases where the user wants the two names to be distinct from one another.
     */
    public String getFile() {
        return StringUtils.isBlank(file) ? getName() : file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    @Override
    public int hashCode() {
        return Objects.hash(description, getOptimisticLockValue(), id, inputsClass, isLocked(),
            getName(), outputsClass, pipelineStepExecutorClass, getVersion());
    }

    @Override
    public boolean totalEquals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PipelineStep other = (PipelineStep) obj;
        boolean equalStep = Objects.equals(description, other.description);
        equalStep = equalStep && getOptimisticLockValue() == other.getOptimisticLockValue();
        equalStep = equalStep && Objects.equals(id, other.id);
        equalStep = equalStep && Objects.equals(inputsClass, other.inputsClass);
        equalStep = equalStep && Objects.equals(getName(), other.getName());
        equalStep = equalStep && Objects.equals(outputsClass, other.outputsClass);
        equalStep = equalStep
            && Objects.equals(pipelineStepExecutorClass, other.pipelineStepExecutorClass);
        return equalStep && getVersion() == other.getVersion();
    }

    @Override
    protected void clearDatabaseId() {
        id = null;
    }

    @Override
    public boolean equals(Object obj) {
        return totalEquals(obj) && ((PipelineStep) obj).isLocked() == isLocked();
    }
}
