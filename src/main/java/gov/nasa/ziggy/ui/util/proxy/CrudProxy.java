package gov.nasa.ziggy.ui.util.proxy;

import java.util.Collection;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.AuditInfo;
import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.services.security.User;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;

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
     * Verify that the currently-logged in User has the proper Privilege to perform the requested
     * operation. Always returns true if there is no logged in user (dev mode)
     *
     * @param requestedOperation
     */
    public static void verifyPrivileges(Privilege requestedOperation) {
        User user = ZiggyGuiConsole.currentUser;

        if (user != null && !user.hasPrivilege(requestedOperation.toString())) {
            throw new ConsoleSecurityException("You do not have permission to perform this action");
        }
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

    /**
     * Update the specified AuditInfo object with the currently logged in user and the current time.
     * Should be called by subclasses when creating/updating entities that have AuditInfo.
     *
     * @param auditInfo
     */
    protected void updateAuditInfo(AuditInfo auditInfo) {
        if (auditInfo == null) {
            log.warn("AuditInfo is null, not updating");
            return;
        }

        User user = ZiggyGuiConsole.currentUser;

        if (user != null) {
            auditInfo.setLastChangedUser(user);
            auditInfo.setLastChangedTime(new Date());
        }
    }
}
