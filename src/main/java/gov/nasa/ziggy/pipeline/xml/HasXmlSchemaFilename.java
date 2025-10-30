package gov.nasa.ziggy.pipeline.xml;

/**
 * Defines a class for which Ziggy can generate a schema and perform unmarshalling validation of a
 * file against that schema.
 *
 * @see xml-schemas.gradle
 * @author PT
 */
public interface HasXmlSchemaFilename {

    /**
     * Filename of the XML schema for this class.
     */
    String getXmlSchemaFilename();
}
