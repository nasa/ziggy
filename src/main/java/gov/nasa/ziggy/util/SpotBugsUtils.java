package gov.nasa.ziggy.util;

/**
 * Utilities for use with the SpotBugs static analysis tool.
 *
 * @author PT
 */
public class SpotBugsUtils {

    /**
     * Justification for suppression of OBJECT_DESERIALIZATION warnings.
     */
    public static final String DESERIALIZATION_JUSTIFICATION = """
        Ziggy only deserializes objects in directories it creates, thus the objects
        that are deserialized are objects that Ziggy initially creates, so there is
        no risk from deserialization.
        """;
}
