package gov.nasa.ziggy.pipeline.xml;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.xml.XMLConstants;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.xml.sax.SAXException;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Marshaller;
import jakarta.xml.bind.Unmarshaller;

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

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public ValidatingXmlManager(Class<T> clazz) {

        try {
            // Generate a schema for validation
            SchemaFactory schemaFactory = SchemaFactory
                .newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            String ziggySchemaDir = DirectoryProperties.ziggySchemaDir().toString();
            String schemaFilename = clazz.getDeclaredConstructor()
                .newInstance()
                .getXmlSchemaFilename();
            Path schemaPath = Paths.get(ziggySchemaDir, "xml", schemaFilename);
            Schema schema = schemaFactory.newSchema(schemaPath.toFile());

            // Generate marshaller and unmarshaller
            JAXBContext context = JAXBContext.newInstance(clazz);
            unmarshaller = context.createUnmarshaller();
            unmarshaller.setSchema(schema);

            marshaller = context.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            marshaller.setSchema(schema);
        } catch (JAXBException | SAXException | InstantiationException | IllegalAccessException
            | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
            | SecurityException e) {
            throw new PipelineException(
                "Unable to instantiate ValidatingXmlManager for " + clazz.getName(), e);
        }
    }

    @SuppressWarnings("unchecked")
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public T unmarshal(File xmlFile) {
        try {
            return (T) unmarshaller.unmarshal(xmlFile);
        } catch (JAXBException e) {
            throw new PipelineException("Unable to unmarshal " + xmlFile.toString(), e);
        }
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public void marshal(T object, File file) {
        try {
            marshaller.marshal(object, file);
        } catch (JAXBException e) {
            throw new PipelineException("Unable to marshal to " + file.toString(), e);
        }
    }
}
