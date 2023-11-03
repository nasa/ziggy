package gov.nasa.ziggy.ui.util.models;

import org.hibernate.Session;

/**
 * Interface implemented by all console Swing models that read their data from the database via
 * Hibernate.
 * <p>
 * Used to notify the models when the Hibernate {@link Session} is closed because of a database
 * error or other reason so they can invalidate/reload their data.
 * <p>
 * 'Long-lived' models (those supporting permanent screens as opposed to model dialogs) should
 * register themselves with the {@link DatabaseModelRegistry} so they will be notified (via this
 * interface) when the model data needs to be invalidated.
 *
 * @author Todd Klaus
 */
public interface ConsoleDatabaseModel {
    /**
     * When called, implementors must discard all Hibernate objects currently held.
     */
    void invalidateModel();
}
