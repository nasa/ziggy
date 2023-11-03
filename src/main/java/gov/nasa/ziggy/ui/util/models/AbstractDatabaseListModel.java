package gov.nasa.ziggy.ui.util.models;

import javax.swing.AbstractListModel;
import javax.swing.ListModel;

import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Super-class for console {@link ListModel}s that read from the database. Provides the basic
 * framework to support model invalidation when the Hibernate {@link Session} is invalidated due to
 * error.
 *
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public abstract class AbstractDatabaseListModel<E> extends AbstractListModel<E>
    implements ConsoleDatabaseModel {
    private static final Logger log = LoggerFactory.getLogger(AbstractDatabaseListModel.class);

    private boolean modelValid = false;

    public AbstractDatabaseListModel() {
    }

    public void register() {
        DatabaseModelRegistry.registerModel(this);
    }

    public abstract void loadFromDatabase();

    @Override
    public void invalidateModel() {
        modelValid = false;
    }

    protected void validityCheck() {
        if (!modelValid) {
            log.info("Model invalid for " + this.getClass().getSimpleName()
                + ", loading data from database...");
            loadFromDatabase();
            modelValid = true;
        }
    }
}
