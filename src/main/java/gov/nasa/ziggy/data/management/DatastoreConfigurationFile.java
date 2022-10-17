package gov.nasa.ziggy.data.management;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.XmlRootElement;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.xml.HasXmlSchemaFilename;
import gov.nasa.ziggy.util.CollectionFilters;

/**
 * Models a single XML file containing definitions of model and data file types. The file contains
 * one or more {@link DataFileType} definitions and one or more {@link ModelType} definitions.
 *
 * @author PT
 */
@XmlRootElement(name = "datastoreConfiguration")
@XmlAccessorType(XmlAccessType.NONE)
public class DatastoreConfigurationFile implements HasXmlSchemaFilename {

    private static final String XML_SCHEMA_FILE_NAME = "datastore-configuration.xsd";

    @XmlElements(value = { @XmlElement(name = "dataFileType", type = DataFileType.class),
        @XmlElement(name = "modelType", type = ModelType.class) })
    private Set<Object> datastoreConfigurationElements = new HashSet<>();

    public Set<Object> getDatastoreConfigurationElements() {
        return datastoreConfigurationElements;
    }

    public void setDatastoreConfigurationElements(Set<Object> datastoreConfigurationElements) {
        Set<Object> originalConfigurationElements = this.datastoreConfigurationElements;
        this.datastoreConfigurationElements = datastoreConfigurationElements;
        if (getDataFileTypes().size() + getModelTypes().size() != datastoreConfigurationElements
            .size()) {
            this.datastoreConfigurationElements = originalConfigurationElements;
            throw new PipelineException("Number of data file types (" + getDataFileTypes().size()
                + ") and number of model types (" + getModelTypes().size()
                + ") does not match number of datastoreConfigurationElements objects ("
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

    @Override
    public String getXmlSchemaFilename() {
        return XML_SCHEMA_FILE_NAME;
    }

}
