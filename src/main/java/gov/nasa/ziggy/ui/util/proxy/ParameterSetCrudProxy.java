package gov.nasa.ziggy.ui.util.proxy;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;

import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.crud.ParameterSetCrud;

/**
 * @author Todd Klaus
 */
public class ParameterSetCrudProxy extends RetrieveLatestVersionsCrudProxy<ParameterSet> {

    public ParameterSetCrudProxy() {
    }

    public void save(final ParameterSet moduleParameterSet) {
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            ParameterSetCrud crud = new ParameterSetCrud();
            crud.persist(moduleParameterSet);
            return null;
        });
    }

    public ParameterSet rename(final ParameterSet parameterSet, final String newName) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            ParameterSetCrud crud = new ParameterSetCrud();
            return crud.rename(parameterSet, newName);
        });
    }

    public List<ParameterSet> retrieveAll() {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            ParameterSetCrud crud = new ParameterSetCrud();
            return crud.retrieveAll();
        });
    }

    public List<ParameterSet> retrieveAllVersionsForName(final String name) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            ParameterSetCrud crud = new ParameterSetCrud();
            return crud.retrieveAllVersionsForName(name);
        });
    }

    public ParameterSet retrieveLatestVersionForName(final String name) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            ParameterSetCrud crud = new ParameterSetCrud();
            return crud.retrieveLatestVersionForName(name);
        });
    }

    @Override
    public List<ParameterSet> retrieveLatestVersions() {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            ParameterSetCrud crud = new ParameterSetCrud();
            return crud.retrieveLatestVersions();
        });
    }

    public void delete(final ParameterSet moduleParameterSet) {
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            ParameterSetCrud crud = new ParameterSetCrud();
            crud.remove(moduleParameterSet);
            return null;
        });
    }

    @Override
    public ParameterSet update(ParameterSet entity) {
        checkArgument(entity instanceof ParameterSet, "entity must be ParameterSet");
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            ParameterSetCrud crud = new ParameterSetCrud();
            return crud.merge(entity);
        });
    }
}
