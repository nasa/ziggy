package gov.nasa.ziggy.ui.models;

import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableModel;

import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Super-class for console {@link TableModel}s that read from the database. Provides the basic
 * framework to support model invalidation when the Hibernate {@link Session} is invalidated due to
 * error.
 *
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public abstract class AbstractDatabaseModel extends AbstractTableModel
    implements ConsoleDatabaseModel {
    private static final Logger log = LoggerFactory.getLogger(AbstractDatabaseModel.class);

    private boolean modelValid = false;
    private boolean performingValidation = false;

    public AbstractDatabaseModel() {
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
        if (performingValidation) {
            return;
        }
        if (!modelValid) {
            performingValidation = true;
            log.debug("Model invalid for " + this.getClass().getSimpleName()
                + ", loading data from database...");
            loadFromDatabase();
            modelValid = true;
            performingValidation = false;
        }
    }

    protected boolean isModelValid() {
        return modelValid;
    }
}
