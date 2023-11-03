package gov.nasa.ziggy.crud;

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
