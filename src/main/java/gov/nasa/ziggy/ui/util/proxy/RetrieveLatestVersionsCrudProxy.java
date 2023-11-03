package gov.nasa.ziggy.ui.util.proxy;

import java.util.List;

/**
 * Abstract class for CRUD proxies that have a {@link #retrieveLatestVersions()} method.
 *
 * @author PT
 */
public abstract class RetrieveLatestVersionsCrudProxy<T> extends CrudProxy<T> {
    public abstract List<T> retrieveLatestVersions();
}
