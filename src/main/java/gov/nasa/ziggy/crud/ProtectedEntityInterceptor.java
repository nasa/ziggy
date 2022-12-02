package gov.nasa.ziggy.crud;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.hibernate.CallbackException;
import org.hibernate.EmptyInterceptor;
import org.hibernate.Transaction;
import org.hibernate.type.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtectedEntityInterceptor extends EmptyInterceptor {

    private static final long serialVersionUID = 1L;

    private static final Logger log = LoggerFactory.getLogger(ProtectedEntityInterceptor.class);

    private static final List<String> protectedPrefixes = new CopyOnWriteArrayList<>();
    static {
        protectedPrefixes.add("gov.nasa.tess.mod.");
    }

    private static final List<String> allowedPrefixes = new CopyOnWriteArrayList<>();

    public static void addAllowedPrefix(String prefix) {
        log.info("Adding allowed prefix for flushed classes: " + prefix);
        allowedPrefixes.add(prefix);
    }

    public static void addPackageForClass(Class<?> clazz) {
        String className = clazz.getName();
        int lastDotPos = className.lastIndexOf('.');
        addAllowedPrefix(className.substring(0, lastDotPos + 1));
    }

    @Override
    public boolean onSave(Object entity, Serializable id, Object[] state, String[] propertyNames,
        Type[] types) {

        checkModificationOK(entity);
        return super.onSave(entity, id, state, propertyNames, types);
    }

    @Override
    public void onDelete(Object entity, Serializable id, Object[] state, String[] propertyNames,
        Type[] types) {

        checkModificationOK(entity);
        super.onDelete(entity, id, state, propertyNames, types);
    }

    @Override
    public boolean onFlushDirty(Object entity, Serializable id, Object[] currentState,
        Object[] previousState, String[] propertyNames, Type[] types) {

        checkModificationOK(entity);
        return super.onFlushDirty(entity, id, currentState, previousState, propertyNames, types);
    }

    @Override
    public void afterTransactionCompletion(Transaction tx) {
        allowedPrefixes.clear();
        super.afterTransactionCompletion(tx);
    }

    private void checkModificationOK(Object entity) {
        String className = entity.getClass().getName();
        if (!flushAllowed(className)) {
            throw new CallbackException("Class is protected from writing: " + className);
        }
    }

    /**
     * Tests whether a flush of an object is allowed. The flush is allowed if the object classname
     * starts with an allowed prefix, or if it does not start with a protected prefix.
     *
     * @param className the classname
     * @return true, if the flush is allowed, false otherwise
     */
    private boolean flushAllowed(String className) {
        for (String prefix : allowedPrefixes) {
            if (className.startsWith(prefix)) {
                return true;
            }
        }

        for (String prefix : protectedPrefixes) {
            if (className.startsWith(prefix)) {
                return false;
            }
        }

        return true;
    }

}
