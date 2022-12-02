package gov.nasa.ziggy.ui.proxy;

import java.util.Collection;
import java.util.Date;

import org.hibernate.FlushMode;
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
public abstract class CrudProxy {
    private static final Logger log = LoggerFactory.getLogger(CrudProxy.class);

    public CrudProxy() {
    }

    /**
     * Called once at startup. Ensures that the database and messaging services used by the executor
     * thread never use XA in the console.
     */
    public static final void initialize() {
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronous(() -> {
            log.info("Setting messaging and database services to NOT use XA");

            DatabaseService databaseService = DatabaseService.getInstance();
            databaseService.getSession().setFlushMode(FlushMode.MANUAL);
        });
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
     * Persist any dirty objects Protected so that sub-classes van verify the correct privileges
     *
     * @throws PipelineException
     */
    protected void saveChanges() {
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronous(() -> {
            DatabaseService databaseService = DatabaseService.getInstance();

            databaseService.beginTransaction();

            databaseService.flush();
            databaseService.commitTransaction();
        });
    }

    /**
     * Proxy method for DatabaseService.evictAll() Uses {@link CrudProxyExecutor} to invoke the
     * {@link DatabaseService} method from the dedicated database thread
     *
     * @param collection
     * @throws PipelineException
     */
    public void evictAll(final Collection<?> collection) throws PipelineException {
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronous(() -> {
            DatabaseService databaseService = DatabaseService.getInstance();
            databaseService.evictAll(collection);
            return null;
        });
    }

    /**
     * Proxy method for DatabaseService.evict() Uses {@link CrudProxyExecutor} to invoke the
     * {@link DatabaseService} method from the dedicated database thread
     *
     * @param object
     */
    public void evict(final Object object) {
        ZiggyGuiConsole.crudProxyExecutor.executeSynchronous(() -> {
            DatabaseService databaseService = DatabaseService.getInstance();
            databaseService.evict(object);
        });
    }
}
