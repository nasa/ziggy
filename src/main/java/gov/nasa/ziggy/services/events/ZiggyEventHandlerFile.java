package gov.nasa.ziggy.services.events;

import java.util.ArrayList;
import java.util.List;

import gov.nasa.ziggy.pipeline.xml.HasXmlSchemaFilename;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;

/**
 * Models an XML file with {@link ZiggyEventHandler} definitions.
 *
 * @author PT
 */
@XmlRootElement(name = "pipelineEventDefinition")
@XmlAccessorType(XmlAccessType.NONE)
public class ZiggyEventHandlerFile implements HasXmlSchemaFilename {

    @XmlElement(name = "pipelineEvent")
    private List<ZiggyEventHandler> ziggyEventHandlers = new ArrayList<>();

    @Override
    public String getXmlSchemaFilename() {
        return ZiggyEventHandler.XML_SCHEMA_FILE_NAME;
    }

    public List<ZiggyEventHandler> getZiggyEventHandlers() {
        return ziggyEventHandlers;
    }

    public void setZiggyEventHandlers(List<ZiggyEventHandler> ziggyEventHandlers) {
        this.ziggyEventHandlers = ziggyEventHandlers;
    }
}
