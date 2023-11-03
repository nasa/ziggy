package gov.nasa.ziggy.pipeline.definition;

import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.InternalParameters;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Embedded;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import jakarta.persistence.UniqueConstraint;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;

/**
 * This class models a set of module parameters. A parameter set may be shared by multiple pipeline
 * modules.
 *
 * @author Todd Klaus
 */
@XmlAccessorType(XmlAccessType.NONE)
@Entity
@Table(name = "ziggy_ParameterSet",
    uniqueConstraints = { @UniqueConstraint(columnNames = { "name", "version" }) })
public class ParameterSet extends UniqueNameVersionPipelineComponent<ParameterSet>
    implements HasGroup {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "ziggy_ParameterSet_generator")
    @SequenceGenerator(name = "ziggy_ParameterSet_generator", initialValue = 1,
        sequenceName = "ziggy_ParameterSet_sequence", allocationSize = 1)
    private Long id;

    @Embedded
    // init with empty placeholder, to be filled in by console
    private AuditInfo auditInfo = new AuditInfo();

    @ManyToOne
    private Group group = null;

    private String description = null;

    @ElementCollection(fetch = FetchType.EAGER)
    @JoinTable(name = "ziggy_ParameterSet_parameters")
    private Set<TypedParameter> typedParameters;

    // Used to support the XML interface for parameter sets
    @XmlAttribute(required = false)
    private String classname = Parameters.class.getName();

    // Used to support the XML interface for parameter sets
    @XmlElement(name = "parameter")
    @Transient
    private Set<Parameter> xmlParameters = new HashSet<>();

    public ParameterSet() {
    }

    public ParameterSet(String name) {
        setName(name);
    }

    public ParameterSet(AuditInfo auditInfo, String name) {
        this.auditInfo = auditInfo;
        setName(name);
    }

    // Populates the XML fields (classname and xmlParameters) from the database fields
    public void populateXmlFields() {
        for (TypedParameter typedProperty : typedParameters) {
            xmlParameters.add(new Parameter(typedProperty));
        }
    }

    // Populates the parameters field from the XML fields.
    public void populateDatabaseFields() throws ClassNotFoundException {
        Set<TypedParameter> typedParameters = new HashSet<>();
        for (Parameter parameter : xmlParameters) {
            typedParameters.add(parameter.typedProperty());
        }
        this.typedParameters = typedParameters;
    }

    /**
     * Construct an instance of the desired {@link Parameters} subclass and populate with values
     * from the typed parameters of the parameter set.
     */
    public <T extends ParametersInterface> T parametersInstance() {
        return parametersInstance(true);
    }

    /**
     * Construct an instance of the desired {@link Parameters} subclass and optionally populate with
     * values from the typed parameters of the parameter set.
     */
    @SuppressWarnings("unchecked")
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public <T extends ParametersInterface> T parametersInstance(boolean populate) {
        T parametersInstance;
        try {
            parametersInstance = (T) Class.forName(classname)
                .getDeclaredConstructor()
                .newInstance();
            if (populate) {
                parametersInstance.populate(typedParameters);
            }
            return parametersInstance;
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
            | InvocationTargetException | NoSuchMethodException | SecurityException
            | ClassNotFoundException e) {
            throw new PipelineException(e);
        }
    }

    /**
     * Populates the fields of the {@link ParameterSet} from an instance of
     * {@link ParametersInterface}.
     */
    public <T extends ParametersInterface> void populateFromParametersInstance(
        T parametersInstance) {
        setTypedParameters(parametersInstance.getParameters());
        setClassname(parametersInstance.getClass().getName());
    }

    public Class<?> clazz() {
        try {
            return Class.forName(classname);
        } catch (ClassNotFoundException e) {
            throw new PipelineException(e);
        }
    }

    /**
     * Determines whether the parameter set contains an instance of {@link Parameters}, or one of
     * {@link InternalParameters}.
     */
    public boolean visibleParameterSet() {
        return !(parametersInstance() instanceof InternalParameters);
    }

    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    public boolean parametersClassDeleted() {
        boolean deleted = false;
        try {
            parametersInstance();
        } catch (PipelineException e) {
            deleted = true;
        }
        return deleted;
    }

    /**
     * Returns true if new fields have been added to the class, but do not exist in the database.
     */
    public <T extends ParametersInterface> boolean hasNewUnsavedFields() {
        T instance = parametersInstance();

        boolean sameKeys = true;
        for (TypedParameter newProperty : instance.getParameters()) {
            if (!typedParameters.contains(newProperty)) {
                sameKeys = false;
            }
        }

        return !sameKeys;
    }

    public Set<TypedParameter> copyOfTypedParameters() {
        Set<TypedParameter> copiedParameters = new TreeSet<>();
        for (TypedParameter typedParameter : typedParameters) {
            copiedParameters.add(new TypedParameter(typedParameter));
        }
        return copiedParameters;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Set<TypedParameter> getTypedParameters() {
        return typedParameters;
    }

    public void setTypedParameters(Set<TypedParameter> typedParameters) {
        this.typedParameters = typedParameters;
        populateXmlFields();
    }

    public AuditInfo getAuditInfo() {
        return auditInfo;
    }

    public void setAuditInfo(AuditInfo auditInfo) {
        this.auditInfo = auditInfo;
    }

    @Override
    public Group group() {
        return group;
    }

    @Override
    public void setGroup(Group group) {
        this.group = group;
    }

    public Long getId() {
        return id;
    }

    public String getClassname() {
        return classname;
    }

    public void setClassname(String classname) {
        this.classname = classname;
    }

    public Set<Parameter> getXmlParameters() {
        return xmlParameters;
    }

    public void setXmlParameters(Set<Parameter> xmlParameters) {
        this.xmlParameters = xmlParameters;
    }

    // For a parameter set, total equals includes the names, types, and values of all TypedParameter
    // instances.
    @Override
    public boolean totalEquals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ParameterSet other = (ParameterSet) obj;
        return Objects.equals(auditInfo, other.auditInfo)
            && Objects.equals(classname, other.classname)
            && Objects.equals(description, other.description) && Objects.equals(group, other.group)
            && Objects.equals(id, other.id) && new TypedParameterCollection(typedParameters)
                .totalEquals(new TypedParameterCollection(other.typedParameters));
    }

    @XmlAccessorType(XmlAccessType.NONE)
    private static class Parameter {

        @XmlAttribute(required = true)
        private String name;

        @XmlAttribute(required = true)
        private String value;

        @XmlAttribute(required = false)
        private String type = "ziggy_string";

        @SuppressWarnings("unused")
        public Parameter() {
        }

        public Parameter(TypedParameter typedProperty) {
            name = typedProperty.getName();
            value = typedProperty.getString();
            type = typedProperty.getDataTypeString();
        }

        public TypedParameter typedProperty() {
            return new TypedParameter(name, value, type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Parameter other = (Parameter) obj;
            return Objects.equals(name, other.name);
        }
    }

    @Override
    protected void clearDatabaseId() {
        id = null;
    }
}
