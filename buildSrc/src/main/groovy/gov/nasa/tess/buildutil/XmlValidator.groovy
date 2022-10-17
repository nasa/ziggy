package gov.nasa.tess.buildutil

import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.util.ArrayList

import javax.xml.XMLConstants
import jakarta.xml.bind.JAXBContext
import jakarta.xml.bind.Unmarshaller
import jakarta.xml.bind.ValidationEvent
import jakarta.xml.bind.ValidationEventLocator
import jakarta.xml.bind.ValidationException
import jakarta.xml.bind.util.JAXBSource
import jakarta.xml.bind.util.ValidationEventCollector
import javax.xml.validation.SchemaFactory
import javax.xml.transform.stream.StreamSource

public class XmlValidator {

    public static final class MessageError extends ValidationException {

        def lineNumber
        def columnNumber

        public MessageError(String message) {
            super(message)
        }

        public MessageError(String message, String errorCode) {
            super(message, errorCode)
        }

        public MessageError(String message, String errorCode, Throwable exception) {
            super(message, errorCode, exception)
        }

        public MessageError(String message, Throwable exception) {
            super(message, exception)
        }

        public MessageError(Throwable exception) {
            super(exception)
        }

        public void setPosition(lineNumber, columnNumber) {
            this.lineNumber = lineNumber
            this.columnNumber = columnNumber
        }
	
        int getLineNumber() {
            return lineNumber
        }
	
        int getColumnNumber() {
            return columnNumber
        }
    }

    public static List<MessageError> validate(File xsdFile, File xmlFile) {

        def errors = new ArrayList<MessageError>()
        def schemaFactory  = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
        def xsdSource
        def xmlSource
        try {
            xsdSource = new StreamSource(new FileReader(xsdFile))
            xmlSource = new StreamSource(new FileReader(xmlFile))
            def schema = schemaFactory.newSchema(xsdSource)
            def validator = schema.newValidator()

            validator.validate(xmlSource)
        } catch (jakarta.xml.bind.UnmarshalException e) {
            MessageError error = new MessageError(e.getMessage(), e.getErrorCode(), e)
            errors[0] = error
        } catch (org.xml.sax.SAXParseException e) {
            MessageError error = new MessageError(e.getMessage(), e)
            error.setPosition(e.getLineNumber(), e.getColumnNumber())
            errors[0] = error
        } finally {
            try {
                if (xsdSource != null) {
                    xsdSource.close()
                }
            } catch (Exception ignore) {}

            try {
                if (xmlSource != null) {
                    xmlSource.close()
                }
            } catch (Exception ignore) {}
        }

        return errors
    }
}
