package gov.nasa.ziggy.pipeline.definition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.xml.HasXmlSchemaFilename;
import gov.nasa.ziggy.util.CollectionFilters;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElements;
import jakarta.xml.bind.annotation.XmlRootElement;

/**
 * Models a single file containing definitions of pipelines and modules. The file contains one or
 * more {@link PipelineModuleDefinition}s and one or more {@link PipelineDefinition}s, each enclosed
 * in an appropriate container.
 *
 * @author PT
 */
@XmlRootElement(name = "pipelineDefinition")
@XmlAccessorType(XmlAccessType.NONE)
public class PipelineDefinitionFile implements HasXmlSchemaFilename {

    private static final String XML_SCHEMA_FILE_NAME = "pipeline-definitions.xsd";

    // This construction allows the user to freely lay down modules and pipelines in any order,
    // but requires a somewhat more convoluted approach to moving the definitions between XML
    // and Java.
    @XmlElements(value = { @XmlElement(name = "module", type = PipelineModuleDefinition.class),
        @XmlElement(name = "pipeline", type = PipelineDefinition.class) })
    private List<Object> pipelineElements = new ArrayList<>();

    public List<Object> getPipelineElements() {
        return pipelineElements;
    }

    public void setPipelineElements(List<Object> pipelineElements) {
        List<Object> originalPipelineElements = this.pipelineElements;
        this.pipelineElements = pipelineElements;
        if (getPipelines().size() + getModules().size() != pipelineElements.size()) {
            this.pipelineElements = originalPipelineElements;
            throw new PipelineException(
                "Number of pipelines (" + getPipelines().size() + ") and number of modules ("
                    + getModules().size() + ") does not match number of pipelineElements objects ("
                    + pipelineElements.size() + ")");
        }
    }

    public List<PipelineModuleDefinition> getModules() {
        return CollectionFilters.filterToList(pipelineElements, PipelineModuleDefinition.class);
    }

    public void setModules(Collection<PipelineModuleDefinition> modules) {
        CollectionFilters.removeTypeFromCollection(pipelineElements,
            PipelineModuleDefinition.class);
        pipelineElements.addAll(modules);
    }

    public List<PipelineDefinition> getPipelines() {
        return CollectionFilters.filterToList(pipelineElements, PipelineDefinition.class);
    }

    public void setPipelines(Collection<PipelineDefinition> pipelines) {
        CollectionFilters.removeTypeFromCollection(pipelineElements, PipelineDefinition.class);
        pipelineElements.addAll(pipelines);
    }

    @Override
    public String getXmlSchemaFilename() {
        return XML_SCHEMA_FILE_NAME;
    }

}
