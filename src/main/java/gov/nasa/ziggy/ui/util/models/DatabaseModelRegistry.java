package gov.nasa.ziggy.ui.util.models;

import java.util.HashSet;
import java.util.Set;

/**
 * 'Long-lived' models (those supporting permanent screens as opposed to model dialogs) should
 * register themselves with this registry so they will be notified (via this interface) when the
 * model data needs to be invalidated.
 *
 * @author Todd Klaus
 */
public class DatabaseModelRegistry {
    private static Set<ConsoleDatabaseModel> registeredModels = new HashSet<>();

    public static synchronized boolean registerModel(ConsoleDatabaseModel model) {
        return registeredModels.add(model);
    }

    public static synchronized boolean unregisterModel(ConsoleDatabaseModel model) {
        return registeredModels.remove(model);
    }

    public static synchronized void invalidateModels() {
        for (ConsoleDatabaseModel model : registeredModels) {
            model.invalidateModel();
        }
    }
}
