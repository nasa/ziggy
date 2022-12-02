package gov.nasa.ziggy.pipeline.definition;

/**
 * A database entity that has an externalId.
 *
 * @author Miles Cote
 */
public interface HasExternalId {
    /**
     * The default value to use when converting to export format.
     */
    long NULL_DATA_SET_ID = 0;

    int externalId();
}
