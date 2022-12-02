package gov.nasa.ziggy.pipeline.xml;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.xml.sax.SAXException;

import gov.nasa.ziggy.services.config.DirectoryProperties;

/**
 * Implements JAXB marshaller and unmarshaller that performs validation against an appropriate XML
 * schema. The class must implement (@link HasXmlSchemaFilename}. These requirements are present
 * because they ensure that a schema with a known location and filename has been generated during
 * Ziggy's build.
 *
 * @author PT
 */
public class ValidatingXmlManager<T extends HasXmlSchemaFilename> {

    private final Unmarshaller unmarshaller;
    private final Marshaller marshaller;

    public ValidatingXmlManager(Class<T> clazz)
        throws InstantiationException, IllegalAccessException, SAXException, JAXBException {

        // Generate a schema for validation
        SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        String ziggySchemaDir = DirectoryProperties.ziggySchemaDir().toString();
        String schemaFilename = clazz.newInstance().getXmlSchemaFilename();
        Path schemaPath = Paths.get(ziggySchemaDir, "xml", schemaFilename);
        Schema schema = schemaFactory.newSchema(schemaPath.toFile());

        // Generate marshaller and unmarshaller
        JAXBContext context = JAXBContext.newInstance(clazz);
        unmarshaller = context.createUnmarshaller();
        unmarshaller.setSchema(schema);

        marshaller = context.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        marshaller.setSchema(schema);
    }

    @SuppressWarnings("unchecked")
    public T unmarshal(File xmlFile) throws JAXBException {
        return (T) unmarshaller.unmarshal(xmlFile);
    }

    public void marshal(T object, File file) throws JAXBException {
        marshaller.marshal(object, file);
    }

}
