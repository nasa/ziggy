package gov.nasa.ziggy.ui.util.proxy;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.database.DatabaseService;

/**
 * Base class for all console CrudProxy classes.
 *
 * @author Todd Klaus
 */
public abstract class CrudProxy<T> {
    private static final Logger log = LoggerFactory.getLogger(CrudProxy.class);

    public CrudProxy() {
    }

    /**
     * Proxy method for DatabaseService.evictAll() Uses {@link CrudProxyExecutor} to invoke the
     * {@link DatabaseService} method from the dedicated database thread
     *
     * @param collection
     * @throws PipelineException
     */
    public void evictAll(final Collection<?> collection) throws PipelineException {
        CrudProxyExecutor.executeSynchronous(() -> {
            DatabaseService.getInstance().evictAll(collection);
        });
    }

    /**
     * Default implementation of an update method. Most {@link CrudProxy} subclasses don't need an
     * update method, hence the default is to throw an exception. Subclasses that do require an
     * update method should override this.
     *
     * @return a merged instance of the parameter that should be used on subsequent operations
     */
    public T update(T entity) {
        throw new UnsupportedOperationException("update method not supported");
    }
}
