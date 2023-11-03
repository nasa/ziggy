package gov.nasa.ziggy.parameters;

import static gov.nasa.ziggy.services.config.PropertyName.HIBERNATE_URL;
import static gov.nasa.ziggy.services.config.PropertyName.HIBERNATE_USERNAME;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.configuration2.ImmutableConfiguration;

import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.xml.HasXmlSchemaFilename;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;

/**
 * Models a single parameter library (defined here as the contents of a single XML file of parameter
 * sets).
 *
 * @author PT
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
public class ParameterLibrary implements HasXmlSchemaFilename {

    private static final String XML_SCHEMA_FILE_NAME = "parameter-library.xsd";

    @XmlAttribute(required = false)
    private String release = "";

    @XmlAttribute(required = false, name = "database-user")
    private String databaseUser = "";

    @XmlAttribute(required = false, name = "override-only")
    private Boolean overrideOnly = false;

    // The following 2 fields were type xs:anyURI in the original XML schema for parameter
    // libraries. I have no idea how to make that work so here I let them be simple
    // Strings.

    @XmlAttribute(required = false, name = "database-url")
    private String databaseUrl = "";

    @XmlElement(name = "parameter-set")
    private List<ParameterSet> parameterSets = new ArrayList<>();

    public ParameterLibrary() {
        ImmutableConfiguration config = ZiggyConfiguration.getInstance();
        release = config.getString(PropertyName.ZIGGY_VERSION.property());
        databaseUrl = config.getString(HIBERNATE_URL.property(), "");
        databaseUser = config.getString(HIBERNATE_USERNAME.property(), "");
        overrideOnly = false;
    }

    public String getRelease() {
        return release;
    }

    public void setRelease(String release) {
        this.release = release;
    }

    public String getDatabaseUser() {
        return databaseUser;
    }

    public void setDatabaseUser(String databaseUser) {
        this.databaseUser = databaseUser;
    }

    public Boolean getOverrideOnly() {
        return overrideOnly;
    }

    public boolean isOverrideOnly() {
        return overrideOnly != null ? overrideOnly : false;
    }

    public void setOverrideOnly(Boolean overrideOnly) {
        this.overrideOnly = overrideOnly;
    }

    public String getDatabaseUrl() {
        return databaseUrl;
    }

    public void setDatabaseUrl(String databaseUrl) {
        this.databaseUrl = databaseUrl;
    }

    public List<ParameterSet> getParameterSets() {
        return parameterSets;
    }

    public void setParameterSets(List<ParameterSet> parameterSets) {
        this.parameterSets = parameterSets;
    }

    public void setDatabaseParameterSets(Collection<ParameterSetDescriptor> descriptors) {
        parameterSets.clear();
        for (ParameterSetDescriptor descriptor : descriptors) {
            parameterSets.add(descriptor.getParameterSet());
        }
    }

    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    public List<ParameterSetDescriptor> getParameterSetDescriptors() {
        List<ParameterSetDescriptor> descriptors = new ArrayList<>();
        for (ParameterSet parameterSet : parameterSets) {
            ParameterSetDescriptor descriptor = new ParameterSetDescriptor(parameterSet.getName(),
                parameterSet.getClassname());
            try {
                parameterSet.populateDatabaseFields();
            } catch (ClassNotFoundException e) {
                // This means that the class name entered in the parameter library
                // XML file doesn't match any class known to the worker. In that
                // case, we don't want to stop reading in parameter sets, we just
                // want to mark that this parameter set requires a nonexistent class.
                descriptor.setState(ParameterSetDescriptor.State.CLASS_MISSING);
            }
            descriptor.setParameterSet(parameterSet);
            descriptors.add(descriptor);
        }
        return descriptors;
    }

    @Override
    public String getXmlSchemaFilename() {
        return XML_SCHEMA_FILE_NAME;
    }
}
