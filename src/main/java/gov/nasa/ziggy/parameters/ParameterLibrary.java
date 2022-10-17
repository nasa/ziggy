package gov.nasa.ziggy.parameters;

import static gov.nasa.ziggy.services.config.PropertyNames.HIBERNATE_URL_PROP_NAME;
import static gov.nasa.ziggy.services.config.PropertyNames.HIBERNATE_USERNAME_PROP_NAME;

import java.util.ArrayList;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.commons.configuration.Configuration;

import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.xml.HasXmlSchemaFilename;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.util.ZiggyVersion;
import javax.xml.bind.annotation.XmlAccessType;

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

    @XmlAttribute(required = false, name = "repository-revision")
    private String repositoryRevision = "";

    @XmlAttribute(required = false, name = "database-user")
    private String databaseUser = "";

    @XmlAttribute(required = false, name = "build-date")
    private XMLGregorianCalendar buildDate;

    @XmlAttribute(required = false, name = "override-only")
    private Boolean overrideOnly = false;

    // The following 2 fields were type xs:anyURI in the original XML schema for parameter
    // libraries. I have no idea how to make that work so here I let them be simple
    // Strings.

    @XmlAttribute(required = false, name = "database-url")
    private String databaseUrl = "";

    @XmlAttribute(required = false, name = "repository-branch")
    private String repositoryBranch = "";

    @XmlElement(name = "parameter-set")
    private List<ParameterSet> parameterSets = new ArrayList<>();

    public ParameterLibrary() throws DatatypeConfigurationException {
        Configuration config = ZiggyConfiguration.getInstance();
        GregorianCalendar gc = new GregorianCalendar();
        gc.setTime(ZiggyVersion.getBuildDate());
        buildDate = DatatypeFactory.newInstance().newXMLGregorianCalendar(gc);
        release = ZiggyVersion.getSoftwareVersion();
        databaseUrl = config.getString(HIBERNATE_URL_PROP_NAME, "");
        databaseUser = config.getString(HIBERNATE_USERNAME_PROP_NAME, "");
        overrideOnly = false;
        repositoryBranch = ZiggyVersion.getBranch();
        repositoryRevision = ZiggyVersion.getRevision();
    }

    public String getRelease() {
        return release;
    }

    public void setRelease(String release) {
        this.release = release;
    }

    public String getRepositoryBranch() {
        return repositoryBranch;
    }

    public void setRepositoryBranch(String repositoryBranch) {
        this.repositoryBranch = repositoryBranch;
    }

    public String getDatabaseUser() {
        return databaseUser;
    }

    public void setDatabaseUser(String databaseUser) {
        this.databaseUser = databaseUser;
    }

    public XMLGregorianCalendar getBuildDate() {
        return buildDate;
    }

    public void setBuildDate(XMLGregorianCalendar buildDate) {
        this.buildDate = buildDate;
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
            parameterSets.add(descriptor.getLibraryParamSet());
        }
    }

    public List<ParameterSetDescriptor> getParameterSetDescriptors() {
        List<ParameterSetDescriptor> descriptors = new ArrayList<>();
        for (ParameterSet parameterSet : parameterSets) {
            ParameterSetDescriptor descriptor = new ParameterSetDescriptor(
                parameterSet.getName().getName(), parameterSet.getClassname());
            try {
                parameterSet.populateDatabaseFields();
                descriptor.setImportedParamsBean(parameterSet.getParameters());
            } catch (ClassNotFoundException e) {
                descriptor.setState(ParameterSetDescriptor.State.CLASS_MISSING);
            }
            descriptors.add(descriptor);
        }
        return descriptors;
    }

    public String getRepositoryRevision() {
        return repositoryRevision;
    }

    public void setRepositoryRevision(String repositoryRevision) {
        this.repositoryRevision = repositoryRevision;
    }

    @Override
    public String getXmlSchemaFilename() {
        return XML_SCHEMA_FILE_NAME;
    }

}
