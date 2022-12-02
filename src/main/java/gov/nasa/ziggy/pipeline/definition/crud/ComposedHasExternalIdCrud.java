package gov.nasa.ziggy.pipeline.definition.crud;

import java.util.List;

import gov.nasa.ziggy.pipeline.definition.HasExternalId;

/**
 * Contains data access operations for an object composed of elements.
 *
 * @author Miles Cote
 */
public interface ComposedHasExternalIdCrud<T extends HasExternalId, E>
    extends HasExternalIdCrud<T> {
    /**
     * Creates the element.
     */
    void createElement(E e);

    /**
     * Retrieves the elements for the external id.
     */
    List<E> retrieveElements(int externalId);
}
