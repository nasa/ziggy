package gov.nasa.ziggy.pipeline.definition;

/**
 * A database entity that can be assigned to a {@link Group}.
 *
 * @author PT
 */
public interface Groupable {

    /**
     * Name of the object in the class that implements {@link Groupable}. Not to be confused with
     * the name of the group itself.
     */
    String getName();
}
