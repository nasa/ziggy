package gov.nasa.ziggy.pipeline.definition.database;

import gov.nasa.ziggy.services.database.AbstractCrud;

/**
 * A simple CRUD class that provides access to {@link AbstractCrud}.
 *
 * @author Bill Wohler
 */
public class SimpleCrud<U> extends AbstractCrud<U> {

    @Override
    public Class<U> componentClass() {
        throw new UnsupportedOperationException(
            "SimpleCrud doesn't support any one particular class");
    }
}
