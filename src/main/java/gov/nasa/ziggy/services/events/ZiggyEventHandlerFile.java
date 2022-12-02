package gov.nasa.ziggy.services.events;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import gov.nasa.ziggy.pipeline.xml.HasXmlSchemaFilename;

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
