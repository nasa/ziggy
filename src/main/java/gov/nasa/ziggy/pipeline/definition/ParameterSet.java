package gov.nasa.ziggy.pipeline.definition;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.persistence.AssociationOverride;
import javax.persistence.CascadeType;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinTable;
import javax.persistence.ManyToOne;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.persistence.Version;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.InternalParameters;
import gov.nasa.ziggy.parameters.Parameters;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

/**
 * This class models a set of module parameters. A parameter set may be shared by multiple pipeline
 * modules.
 *
 * @author Todd Klaus
 */
@XmlAccessorType(XmlAccessType.NONE)
@Entity
@Table(name = "PI_PS")
public class ParameterSet {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "sg")
    @SequenceGenerator(name = "sg", initialValue = 1, sequenceName = "PI_PS_SEQ",
        allocationSize = 1)
    private long id;

    @Embedded
    // init with empty placeholder, to be filled in by console
    private AuditInfo auditInfo = new AuditInfo();

    // Combination of name+version must be unique (see shared-extra-ddl-create.sql)
    @XmlAttribute(required = true)
    @XmlJavaTypeAdapter(ParameterSetName.ParameterSetNameAdapter.class)
    @ManyToOne(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    private ParameterSetName name;

    @XmlAttribute(required = false)
    private Integer version = 0;

    /**
     * used by Hibernate to implement optimistic locking. Should prevent 2 different console users
     * from clobbering each others changes
     */
    @Version
    private int dirty = 0;

    @ManyToOne
    private Group group = null;

    /**
     * Set to true when the first pipeline instance is created using this definition in order to
     * preserve the data accountability record. Editing a locked definition will result in a new,
     * unlocked instance with the version incremented
     */
    @XmlAttribute(required = false)
    private Boolean locked = false;

    private String description = null;

    @Embedded
    @AssociationOverride(name = "typedProperties", joinTable = @JoinTable(name = "PI_PS_PROPS"))
    private BeanWrapper<Parameters> parameters = null;

    // Used to support the XML interface for parameter sets
    @XmlAttribute(required = false)
    @Transient
    private String classname = "gov.nasa.ziggy.parameters.DefaultParameters";

    // Used to support the XML interface for parameter sets
    @XmlElement(name = "parameter")
    @Transient
    private Set<Parameter> xmlParameters = new HashSet<>();

    public ParameterSet() {
    }

    public ParameterSet(String name) {
        this.name = new ParameterSetName(name);
    }

    public ParameterSet(AuditInfo auditInfo, String name) {
        this.auditInfo = auditInfo;
        this.name = new ParameterSetName(name);
    }

    /**
     * Copy constructor
     */
    public ParameterSet(ParameterSet other) {
        this(other, false);
    }

    ParameterSet(ParameterSet other, boolean exact) {
        auditInfo = other.auditInfo;
        name = other.name;
        group = other.group;
        description = other.description;
        parameters = new BeanWrapper<>(other.parameters);

        if (exact) {
            version = other.version;
            locked = other.locked;
        } else {
            version = 0;
            locked = false;
        }
    }

    public void rename(String name) {
        this.name = new ParameterSetName(name);
    }

    public ParameterSet newVersion() throws PipelineException {
        if (!locked) {
            throw new PipelineException("Can't version an unlocked instance");
        }

        ParameterSet copy = new ParameterSet(this);
        copy.version = version + 1;

        return copy;
    }

    // Populates the XML fields (classname and xmlParameters) from the database fields
    public void populateXmlFields() {
        classname = parameters.getClassName();
        for (TypedParameter typedProperty : parameters.typedProperties) {
            xmlParameters.add(new Parameter(typedProperty));
        }
    }

    // populates the parameters field from the XML fields.
    @SuppressWarnings("unchecked")
    public void populateDatabaseFields() throws ClassNotFoundException {
        Class<? extends Parameters> beanClass;
        beanClass = (Class<? extends Parameters>) Class.forName(classname);
        BeanWrapper<Parameters> paramsBean = new BeanWrapper<>(beanClass);
        Set<TypedParameter> typedProperties = new HashSet<>();
        for (Parameter parameter : xmlParameters) {
            typedProperties.add(parameter.typedProperty());
        }
        paramsBean.setTypedProperties(typedProperties);
        parameters = paramsBean;
    }

    /**
     * just set locked = true
     */
    public void lock() {
        locked = true;
    }

    /**
     * Determines whether the parameter set contains an instance of {@link Parameters}, or one of
     * {@link InternalParameters}.
     */
    public boolean visibleParameterSet() {
        return !(parameters.getInstance() instanceof InternalParameters);
    }

    /**
     * @return Returns the description.
     */
    public String getDescription() {
        return description;
    }

    /**
     * @param description The description to set.
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * @return Returns the name.
     */
    public ParameterSetName getName() {
        return name;
    }

    /**
     * @return Returns the version.
     */
    public int getVersion() {
        return version;
    }

    /**
     * @return the parameters
     */
    public BeanWrapper<Parameters> getParameters() {
        return parameters;
    }

    @SuppressWarnings("unchecked")
    public <T extends Parameters> T parametersInstance() {
        return (T) getParameters().getInstance();
    }

    public boolean parametersClassDeleted() {
        boolean deleted = false;
        try {
            parameters.getInstance();
        } catch (PipelineException e) {
            deleted = true;
        }
        return deleted;
    }

    /**
     * @param parameters the parameters to set
     */
    public void setParameters(BeanWrapper<Parameters> parameters) {
        this.parameters = parameters;
        populateXmlFields();
    }

    public AuditInfo getAuditInfo() {
        return auditInfo;
    }

    public void setAuditInfo(AuditInfo auditInfo) {
        this.auditInfo = auditInfo;
    }

    public boolean isLocked() {
        return locked;
    }

    @Override
    public String toString() {
        return name != null ? name.getName() : "UNNAMED";
    }

    public Group getGroup() {
        return group;
    }

    public void setGroup(Group group) {
        this.group = group;
    }

    public long getId() {
        return id;
    }

    /**
     * For TEST USE ONLY
     */
    public void setDirty(int dirty) {
        this.dirty = dirty;
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
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Parameter other = (Parameter) obj;
            return Objects.equals(name, other.name);
        }

    }

}
