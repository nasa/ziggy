package gov.nasa.ziggy.pipeline.definition.importer;

import java.util.ArrayList;
import java.util.List;

import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.data.datastore.DatastoreNode;
import gov.nasa.ziggy.data.datastore.DatastoreRegexp;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.Pipeline;
import gov.nasa.ziggy.pipeline.step.PipelineStep;
import gov.nasa.ziggy.pipeline.step.remote.RemoteEnvironment;
import gov.nasa.ziggy.pipeline.xml.HasXmlSchemaFilename;
import gov.nasa.ziggy.services.events.ZiggyEventHandler;
import gov.nasa.ziggy.util.CollectionFilters;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElements;
import jakarta.xml.bind.annotation.XmlRootElement;

/**
 * Models a single XML file containing pipeline definitions. Pipeline definitions can include the
 * following:
 * <ol>
 * <li>{@link PipelineStep} instances, with XML type "step"
 * <li>{@link Pipeline} instances with XML type "pipeline"
 * <li>{@link ParameterSet} instances, with XML type "parameterSet"
 * <li>{@link DataFileType} instances, with XML type "dataFileType"
 * <li>{@link ModelType} instances, with XML type "modelType"
 * <li>{@link DatastoreRegexp} instances, with XML type "datastoreRegexp"
 * <li>{@link DatastoreNode} instances, with XML type "datastoreNode"
 * <li>{@link RemoteEnvironment} instances, with XML type "remoteEnvironment"
 * <li>{@link ZiggyEventHandler} instances, with XML type "pipelineEvent".
 * </ol>
 * <p>
 * The {@link PipelineDefinitionImporter} reads one or more files with XML definition of the data
 * types listed above and persists them to the database.
 *
 * @author PT
 */
@XmlRootElement(name = "pipelineDefinition")
@XmlAccessorType(XmlAccessType.NONE)
public class PipelineDefinitionFile implements HasXmlSchemaFilename {

    /**
     * A marker interface that denotes elements of a pipeline definition.
     *
     * @author Bill Wohler
     */
    public interface PipelineDefinitionElement {
    }

    /**
     * Element types contained within a pipeline definition file.
     *
     * @author Bill Wohler
     */
    public enum PipelineDefinitionType {
        STEP(PipelineStep.class),
        PIPELINE(Pipeline.class),
        PARAMETER_SET(ParameterSet.class),
        DATA_FILE_TYPE(DataFileType.class),
        MODEL_TYPE(ModelType.class),
        DATASTORE_REGEXP(DatastoreRegexp.class),
        DATASTORE_NODE(DatastoreNode.class),
        REMOTE_ENVIRONMENT(RemoteEnvironment.class),
        PIPELINE_EVENT(ZiggyEventHandler.class);

        private Class<? extends PipelineDefinitionElement> type;

        PipelineDefinitionType(Class<? extends PipelineDefinitionElement> type) {
            this.type = type;
        }

        public Class<? extends PipelineDefinitionElement> getType() {
            return type;
        }
    }

    private static final String XML_SCHEMA_FILE_NAME = "pipeline-definitions.xsd";

    // Update PipelineDefinitionType whenever this list is modified.
    @XmlElements(value = { @XmlElement(name = "dataFileType", type = DataFileType.class),
        @XmlElement(name = "datastoreNode", type = DatastoreNode.class),
        @XmlElement(name = "datastoreRegexp", type = DatastoreRegexp.class),
        @XmlElement(name = "modelType", type = ModelType.class),
        @XmlElement(name = "parameterSet", type = ParameterSet.class),
        @XmlElement(name = "pipeline", type = Pipeline.class),
        @XmlElement(name = "pipelineEvent", type = ZiggyEventHandler.class),
        @XmlElement(name = "remoteEnvironment", type = RemoteEnvironment.class),
        @XmlElement(name = "step", type = PipelineStep.class) })
    private List<PipelineDefinitionElement> pipelineElements = new ArrayList<>();

    public List<PipelineDefinitionElement> getPipelineElements() {
        return pipelineElements;
    }

    @SuppressWarnings("unchecked")
    public <T extends PipelineDefinitionElement> List<T> getPipelineElements(
        PipelineDefinitionType type) {
        return (List<T>) CollectionFilters.filterToList(pipelineElements, type.getType());
    }

    @Override
    public String getXmlSchemaFilename() {
        return XML_SCHEMA_FILE_NAME;
    }
}
