package gov.nasa.ziggy.pipeline.xml;

/**
 * Defines a class for which Ziggy can generate a schema and perform unmarshalling validation of a
 * file against that schema.
 * <p>
 * NB: If you add a new class that implements {@link HasXmlSchemaFilename}, you must add it to the
 * list of classes returned by {@link XmlSchemaExporter#schemaClasses()}. That will ensure that the
 * schema for the class is automatically generated at build time.
 *
 * @author PT
 */
public interface HasXmlSchemaFilename {

    /**
     * Filename of the XML schema for this class.
     */
    String getXmlSchemaFilename();
}
