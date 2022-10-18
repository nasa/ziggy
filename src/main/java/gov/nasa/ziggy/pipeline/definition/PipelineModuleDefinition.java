package gov.nasa.ziggy.pipeline.definition;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.persistence.AttributeOverride;
import javax.persistence.AttributeOverrides;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.persistence.Version;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import gov.nasa.ziggy.module.DefaultPipelineInputs;
import gov.nasa.ziggy.module.DefaultPipelineOutputs;
import gov.nasa.ziggy.module.ExternalProcessPipelineModule;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.module.PipelineInputs;
import gov.nasa.ziggy.module.PipelineOutputs;
import gov.nasa.ziggy.parameters.Parameters;

/**
 * This class models a pipeline module, which consists of an algorithm and the parameters that
 * control the behavior of that algorithm.
 *
 * @author Todd Klaus
 */
@XmlAccessorType(XmlAccessType.NONE)
@Entity
@Table(name = "PI_MOD_DEF")
public class PipelineModuleDefinition {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "sg")
    @SequenceGenerator(name = "sg", initialValue = 1, sequenceName = "PI_MOD_DEF_SEQ",
        allocationSize = 1)
    private long id;

    // Combination of name+version must be unique (see shared-extra-ddl-create.sql)
    @ManyToOne(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    @XmlAttribute(required = true)
    @XmlJavaTypeAdapter(ModuleName.ModuleNameAdapter.class)
    private ModuleName name;
    private int version = 0;

    @ManyToOne
    private Group group = null;

    /**
     * Set to true when the first pipeline instance is created using this definition in order to
     * preserve the data accountability record. Editing a locked definition will result in a new,
     * unlocked instance with the version incremented
     */
    private boolean locked = false;

    @Embedded
    // init with empty placeholder, to be filled in by console
    private AuditInfo auditInfo = new AuditInfo();

    /**
     * used by Hibernate to implement optimistic locking. Should prevent 2 different console users
     * from clobbering each others changes
     */
    @Version
    private int dirty = 0;

    @XmlAttribute
    private String description = "description";

    @XmlAttribute(required = false)
    @XmlJavaTypeAdapter(ClassWrapper.ClassWrapperAdapter.class)
    @Embedded
    @AttributeOverrides({
        @AttributeOverride(name = "clazz", column = @Column(name = "MODULE_CLASS")),
        @AttributeOverride(name = "initialized",
            column = @Column(name = "IMP_CLASS_INITIALIZED")) })
    private ClassWrapper<PipelineModule> pipelineModuleClass = new ClassWrapper<>(
        ExternalProcessPipelineModule.class);

    @Embedded
    @XmlAttribute(required = false)
    @XmlJavaTypeAdapter(ClassWrapper.ClassWrapperAdapter.class)
    @AttributeOverrides({
        @AttributeOverride(name = "clazz", column = @Column(name = "INPUTS_CLASS")),
        @AttributeOverride(name = "initialized",
            column = @Column(name = "INPUTS_CLASS_INITIALIZED")) })
    private ClassWrapper<PipelineInputs> inputsClass = new ClassWrapper<>(
        DefaultPipelineInputs.class);

    @Embedded
    @XmlAttribute(required = false)
    @XmlJavaTypeAdapter(ClassWrapper.ClassWrapperAdapter.class)
    @AttributeOverrides({
        @AttributeOverride(name = "clazz", column = @Column(name = "OUTPUTS_CLASS")),
        @AttributeOverride(name = "initialized",
            column = @Column(name = "OUTPUTS_CLASS_INITIALIZED")) })
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
        this.name = new ModuleName(name);
    }

    public PipelineModuleDefinition(AuditInfo auditInfo, String name) {
        this.auditInfo = auditInfo;
        this.name = new ModuleName(name);
    }

    /**
     * Copy constructor
     *
     * @param other
     */
    public PipelineModuleDefinition(PipelineModuleDefinition other) {
        this(other, false);
    }

    PipelineModuleDefinition(PipelineModuleDefinition other, boolean exact) {
        name = other.name;
        group = other.group;
        auditInfo = other.auditInfo;
        description = other.description;
        pipelineModuleClass = other.pipelineModuleClass;
        exeTimeoutSecs = other.exeTimeoutSecs;
        minMemoryMegaBytes = other.minMemoryMegaBytes;

        if (exact) {
            version = other.version;
            locked = other.locked;
        } else {
            version = 0;
            locked = false;
        }
    }

    public void rename(String name) {
        this.name = new ModuleName(name);
    }

    public PipelineModuleDefinition newVersion() {
        if (!locked) {
            throw new PipelineException("Can't version an unlocked instance");
        }

        PipelineModuleDefinition copy = new PipelineModuleDefinition(this);
        copy.version = version + 1;

        return copy;
    }

    /**
     * Lock this and all associated parameter sets
     * <p>
     * Before locking, if currently NOT locked, replace all param sets with the latest version, then
     * lock those, then this one. This guarantees that when a pipeline is launched, the latest
     * available params are used.
     *
     * <pre>
     * if !locked
     *   lock data object
     *   for each param set
     *     update param set with latest version
     *     lock latest version param set
     *   locked = true
     * </pre>
     *
     * @throws PipelineException
     */
    public void lock() {
        locked = true;
    }

    public boolean isLocked() {
        return locked;
    }

    public ModuleName getName() {
        return name;
    }

    public int getVersion() {
        return version;
    }

    /**
     * @return Returns the id.
     */
    public long getId() {
        return id;
    }

    @Override
    public String toString() {
        return name.toString();
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

    public Set<ClassWrapper<Parameters>> getRequiredParameterClasses() {
        PipelineInputs pipelineInputs = inputsClass.newInstance();

        Set<ClassWrapper<Parameters>> requiredParameters = new HashSet<>();
        List<Class<? extends Parameters>> moduleParameters = pipelineInputs.requiredParameters();

        for (Class<? extends Parameters> clazz : moduleParameters) {
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

    public int getDirty() {
        return dirty;
    }

    /**
     * For TEST USE ONLY
     */
    public void setDirty(int dirty) {
        this.dirty = dirty;
    }

    @Override
    public int hashCode() {
        return Objects.hash(description, dirty, exeTimeoutSecs, group, id, inputsClass, locked,
            minMemoryMegaBytes, name, outputsClass, pipelineModuleClass, version);
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
        PipelineModuleDefinition other = (PipelineModuleDefinition) obj;
        boolean equalModule = Objects.equals(description, other.description);
        equalModule = equalModule && dirty == other.dirty;
        equalModule = equalModule && exeTimeoutSecs.intValue() == other.exeTimeoutSecs.intValue();
        equalModule = equalModule && Objects.equals(group, other.group);
        equalModule = equalModule && id == other.id;
        equalModule = equalModule && Objects.equals(inputsClass, other.inputsClass);
        equalModule = equalModule && locked == other.locked;
        equalModule = equalModule
            && minMemoryMegaBytes.intValue() == other.minMemoryMegaBytes.intValue();
        equalModule = equalModule && Objects.equals(name, other.name);
        equalModule = equalModule && Objects.equals(outputsClass, other.outputsClass);
        equalModule = equalModule && Objects.equals(pipelineModuleClass, other.pipelineModuleClass);
        equalModule = equalModule && version == other.version;
        return equalModule;
    }

}
