package gov.nasa.ziggy.pipeline.definition;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;

import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinTable;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import jakarta.persistence.UniqueConstraint;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;

/**
 * This class models a set of algorithm parameters. A parameter set may be shared by multiple
 * pipeline modules.
 *
 * @author Todd Klaus
 * @author PT
 */
@XmlAccessorType(XmlAccessType.NONE)
@Entity
@Table(name = "ziggy_ParameterSet",
    uniqueConstraints = { @UniqueConstraint(columnNames = { "name", "version" }) })
public class ParameterSet extends UniqueNameVersionPipelineComponent<ParameterSet>
    implements Groupable {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "ziggy_ParameterSet_generator")
    @SequenceGenerator(name = "ziggy_ParameterSet_generator", initialValue = 1,
        sequenceName = "ziggy_ParameterSet_sequence", allocationSize = 1)
    private Long id;

    private String description = null;

    @ElementCollection(fetch = FetchType.EAGER)
    @JoinTable(name = "ziggy_ParameterSet_parameters")
    private Set<Parameter> parameters = new HashSet<>();

    // Used to support the XML interface for parameter sets
    @XmlElement(name = "parameter")
    @Transient
    private Set<XmlParameter> xmlParameters = new HashSet<>();

    @XmlAttribute(required = false)
    private String moduleInterfaceName;

    public ParameterSet() {
    }

    public ParameterSet(String name) {
        setName(name);
    }

    public ParameterSet(ParameterSet parameterSet) {
        setName(parameterSet.getName());
        setParameters(parameterSet.copyOfParameters());
    }

    // Populates the XML fields (classname and xmlParameters) from the database fields
    public void populateXmlFields() {
        for (Parameter parameter : parameters) {
            xmlParameters.add(new XmlParameter(parameter));
        }
    }

    // Populates the parameters field from the XML fields.
    public void populateDatabaseFields() throws ClassNotFoundException {
        Set<Parameter> parameters = new HashSet<>();
        for (XmlParameter parameter : xmlParameters) {
            parameters.add(parameter.typedProperty());
        }
        this.parameters = parameters;
    }

    public Set<Parameter> copyOfParameters() {
        Set<Parameter> copiedParameters = new TreeSet<>();
        for (Parameter parameter : parameters) {
            copiedParameters.add(new Parameter(parameter));
        }
        return copiedParameters;
    }

    public Map<String, Parameter> parameterByName() {
        Map<String, Parameter> parametersByName = new HashMap<>();
        for (Parameter parameter : getParameters()) {
            parametersByName.put(parameter.getName(), parameter);
        }
        return parametersByName;
    }

    /**
     * Converts a {@link Set} of {@link ParameterSet} instances to a {@link Map} with the parameter
     * set names as keys.
     */
    public static Map<String, ParameterSet> parameterSetByName(
        Collection<ParameterSet> parameterSets) {
        Map<String, ParameterSet> parameterSetsByName = new HashMap<>();
        for (ParameterSet parameterSet : parameterSets) {
            parameterSetsByName.put(parameterSet.getName(), parameterSet);
        }
        return parameterSetsByName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Set<Parameter> getParameters() {
        return parameters;
    }

    public void setParameters(Set<Parameter> parameters) {
        this.parameters = parameters;
    }

    /**
     * Returns the module interface name for the parameter set, if it is assigned; otherwise the
     * parameter set name itself is returned.
     */
    public String getModuleInterfaceName() {
        return StringUtils.isEmpty(moduleInterfaceName) ? getName() : moduleInterfaceName;
    }

    public void setModuleInterfaceName(String moduleInterfaceName) {
        this.moduleInterfaceName = moduleInterfaceName;
    }

    @Override
    public Long getId() {
        return id;
    }

    // For a parameter set, total equals includes the names, types, and values of all Parameter
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
        return Objects.equals(description, other.description) && Objects.equals(id, other.id)
            && Parameter.identicalParameters(parameters, other.parameters);
    }

    @XmlAccessorType(XmlAccessType.NONE)
    private static class XmlParameter {

        @XmlAttribute(required = true)
        private String name;

        @XmlAttribute(required = true)
        private String value;

        @XmlAttribute(required = false)
        private String type = "ziggy_string";

        @SuppressWarnings("unused")
        public XmlParameter() {
        }

        public XmlParameter(Parameter parameter) {
            name = parameter.getName();
            value = parameter.getString();
            type = parameter.getDataTypeString();
        }

        public Parameter typedProperty() {
            return new Parameter(name, value, type);
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
            XmlParameter other = (XmlParameter) obj;
            return Objects.equals(name, other.name);
        }
    }

    @Override
    protected void clearDatabaseId() {
        id = null;
    }
}
