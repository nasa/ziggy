package gov.nasa.ziggy.data.datastore;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.xml.HasXmlSchemaFilename;
import gov.nasa.ziggy.util.CollectionFilters;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElements;
import jakarta.xml.bind.annotation.XmlRootElement;

/**
 * Models a single XML file containing definitions of model and data file types, datastore directory
 * regular expressions, and datastore nodes. The file contains {@link DataFileType},
 * {@link ModelType}, {@link DatastoreRegexp}, and {@link DatastoreNode} definitions.
 *
 * @author PT
 */
@XmlRootElement(name = "datastoreConfiguration")
@XmlAccessorType(XmlAccessType.NONE)
public class DatastoreConfigurationFile implements HasXmlSchemaFilename {

    private static final String XML_SCHEMA_FILE_NAME = "datastore-configuration.xsd";

    @XmlElements(value = { @XmlElement(name = "dataFileType", type = DataFileType.class),
        @XmlElement(name = "modelType", type = ModelType.class),
        @XmlElement(name = "datastoreRegexp", type = DatastoreRegexp.class),
        @XmlElement(name = "datastoreNode", type = DatastoreNode.class) })
    private Set<Object> datastoreConfigurationElements = new HashSet<>();

    public Set<Object> getDatastoreConfigurationElements() {
        return datastoreConfigurationElements;
    }

    public void setDatastoreConfigurationElements(Set<Object> datastoreConfigurationElements) {
        Set<Object> originalConfigurationElements = this.datastoreConfigurationElements;
        this.datastoreConfigurationElements = datastoreConfigurationElements;
        if (getDataFileTypes().size() + getModelTypes().size() + getRegexps().size()
            + getDatastoreNodes().size() != datastoreConfigurationElements.size()) {
            this.datastoreConfigurationElements = originalConfigurationElements;
            throw new PipelineException("Number of data file types (" + getDataFileTypes().size()
                + "), number of model types (" + getModelTypes().size() + "), number of regexps ("
                + getRegexps().size() + "), number of datastore nodes ("
                + getDatastoreNodes().size() + ") total "
                + (getDataFileTypes().size() + getModelTypes().size() + getRegexps().size()
                    + getDatastoreNodes().size())
                + " does not match number of datastoreConfigurationElements objects ("
                + datastoreConfigurationElements.size() + ")");
        }
    }

    public Set<DataFileType> getDataFileTypes() {
        return CollectionFilters.filterToSet(datastoreConfigurationElements, DataFileType.class);
    }

    public void setDataFileTypes(Collection<DataFileType> dataFileTypes) {
        CollectionFilters.removeTypeFromCollection(datastoreConfigurationElements,
            DataFileType.class);
        datastoreConfigurationElements.addAll(dataFileTypes);
    }

    public Set<ModelType> getModelTypes() {
        return CollectionFilters.filterToSet(datastoreConfigurationElements, ModelType.class);
    }

    public void setModelTypes(Collection<ModelType> modelTypes) {
        CollectionFilters.removeTypeFromCollection(datastoreConfigurationElements, ModelType.class);
        datastoreConfigurationElements.addAll(modelTypes);
    }

    public Set<DatastoreRegexp> getRegexps() {
        return CollectionFilters.filterToSet(datastoreConfigurationElements, DatastoreRegexp.class);
    }

    public void setRegexps(Collection<DatastoreRegexp> regexps) {
        CollectionFilters.removeTypeFromCollection(datastoreConfigurationElements,
            DatastoreRegexp.class);
        datastoreConfigurationElements.addAll(regexps);
    }

    public Set<DatastoreNode> getDatastoreNodes() {
        return CollectionFilters.filterToSet(datastoreConfigurationElements, DatastoreNode.class);
    }

    public void setDatastoreNodes(Collection<DatastoreNode> datastoreNodes) {
        CollectionFilters.removeTypeFromCollection(datastoreConfigurationElements,
            DatastoreNode.class);
        datastoreConfigurationElements.addAll(datastoreNodes);
    }

    @Override
    public String getXmlSchemaFilename() {
        return XML_SCHEMA_FILE_NAME;
    }
}
