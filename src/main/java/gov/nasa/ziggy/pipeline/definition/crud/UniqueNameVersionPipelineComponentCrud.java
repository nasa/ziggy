package gov.nasa.ziggy.pipeline.definition.crud;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.UniqueNameVersionPipelineComponent;
import gov.nasa.ziggy.pipeline.definition.UniqueNameVersionPipelineComponent_;

/**
 * Superclass for CRUD methods that support {@link UniqueNameVersionPipelineComponent} classes.
 * <p>
 * Subclasses of {@link UniqueNameVersionPipelineComponent} have some specialized rules for their
 * database tables. Most relevant is that any changes made to an instance that is locked cannot be
 * persisted to the locked instance in the database, but must be saved as an unlocked instance with
 * a new version number. The methods of this class ensure that these requirements are met.
 *
 * @author PT
 */
public abstract class UniqueNameVersionPipelineComponentCrud<U extends UniqueNameVersionPipelineComponent<U>>
    extends AbstractCrud<U> {

    /**
     * Populates XML fields for an instance.
     * <p>
     * The issue here is that the instance fields that are used by the database and the instance
     * fields that are used when serializing to XML are not the same. Classes for which this is true
     * require logic that populates the latter from the former, which in turn means that the CRUD
     * class must also populate the latter from the former so that an instance that's returned from
     * a CRUD class is ready to be serialized if so desired.
     */
    protected abstract void populateXmlFields(Collection<U> objects);

    /**
     * Retrieves the latest version for a given name.
     */
    public U retrieveLatestVersionForName(String name) {

        ZiggyQuery<U, U> query = createZiggyQuery(componentClass());
        ZiggyQuery<U, Integer> versionQuery = query.ziggySubquery(componentClass(), Integer.class);
        versionQuery.column(UniqueNameVersionPipelineComponent_.NAME).in(name);
        versionQuery.column(UniqueNameVersionPipelineComponent_.VERSION).max();
        query.column(UniqueNameVersionPipelineComponent_.NAME).in(name);
        query.column(UniqueNameVersionPipelineComponent_.VERSION).in(versionQuery);
        U result = uniqueResult(query);
        if (result == null) {
            return null;
        }
        populateXmlFields(List.of(result));

        return result;
    }

    /**
     * Retrieve all database instances for the specified name.
     */
    public List<U> retrieveAllVersionsForName(String name) {
        ZiggyQuery<U, U> query = createZiggyQuery(componentClass());
        query.column(UniqueNameVersionPipelineComponent_.NAME).in(name);
        List<U> results = list(query);
        populateXmlFields(results);
        return results;
    }

    /**
     * Retrieves the unique list of names of all instances.
     */
    public List<String> retrieveNames() {
        ZiggyQuery<U, String> query = createZiggyQuery(componentClass(), String.class);
        query.column(UniqueNameVersionPipelineComponent_.NAME)
            .select()
            .distinct(true)
            .ascendingOrder();
        return list(query);
    }

    /**
     * Retrieves the latest versions of the class across all names in use in the database.
     */
    public List<U> retrieveLatestVersions() {
        List<String> names = retrieveNames();
        List<U> results = new ArrayList<>();
        for (String name : names) {
            results.add(retrieveLatestVersionForName(name));
        }
        populateXmlFields(results);
        return results;
    }

    /**
     * Renames an instance. The new name must not be a name that is already in use in the database.
     * Because the object is saved to a new name, its version is set to zero and its lock status is
     * set to unlocked.
     *
     * @return the merged instance, which should be used in lieu of {@code pipelineComponent}
     */
    public U rename(U pipelineComponent, String newName) {
        String oldName = pipelineComponent.getName();

        // If the name is changed, the new name isn't allowed to conflict with
        // any existing pipeline definition names.
        if (!newName.equals(oldName) && retrieveLatestVersionForName(newName) != null) {
            throw new PipelineException("Unable to change name of "
                + componentNameForExceptionMessages() + " from " + oldName + " to " + newName
                + " due to conflict with existing definition in database");
        }

        pipelineComponent = pipelineComponent.rename(newName);
        return super.merge(pipelineComponent);
    }

    /**
     * Persists or merges an object into the database, as appropriate, with persist semantics. That
     * means that updates to the persisted object will be persisted as well.
     *
     * @see #merge(Object)
     */
    // TODO If this method calls merge, it must return the merged object!
    // Note that this note was added in a commit where this call was made and the parameter o was
    // later used in a merge() call. The merge() call created a second object and subsequently
    // caused exceptions when uniqueResult() was called.
    @SuppressWarnings("unchecked")
    @Override
    public void persist(Object o) {
        if (!(o instanceof UniqueNameVersionPipelineComponent)) {
            super.merge(o);
            return;
        }
        persistOrMerge((U) o);
    }

    /**
     * Persists or merges an object into the database, as appropriate, with merge semantics. That
     * means that updates to the persisted object are not guaranteed to be persisted; update the
     * returned object instead.
     * <p>
     * If the database currently contains no objects that share the name of the argument object, the
     * argument object is created via {@link AbstractCrud#persist(Object)} and the original object
     * is returned. If the database contains an object that shares the name, then one of two things
     * happens:
     * <ol>
     * <li>If the instance in the database is unlocked, the argument object is merged into the
     * database object via {@link AbstractCrud#merge(Object)}. The persistent instance is returned.
     * <li>If the instance in the database is locked, the argument object's version number is set 1
     * higher than that of the database object, and the new object is persisted via
     * {@link AbstractCrud#merge(Object)}. The persistent instance is returned. Note that in this
     * case, we're taking advantage of the fact that merge can also be used to persist an object in
     * the database, with some caveats.
     * </ol>
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T merge(T o) {
        if (!(o instanceof UniqueNameVersionPipelineComponent)) {
            return super.merge(o);
        }
        return (T) persistOrMerge((U) o);
    }

    private U persistOrMerge(U pipelineComponent) {

        U latestVersion = retrieveLatestVersionForName(pipelineComponent.getName());

        // If there's nothing at all in the database, we persist.
        if (latestVersion == null) {
            pipelineComponent.updateAuditInfo();
            super.persist(pipelineComponent);
            return pipelineComponent;
        }

        // If the latest version and the pipelineComponent are identical, there's
        // no need to do anything.
        if (pipelineComponent.totalEquals(latestVersion)) {
            return pipelineComponent;
        }

        // If there's an instance in the database, we take the one that needs to go
        // to the database and set its version as needed; then merge it.
        U unlockedVersion = pipelineComponent.unlockedVersion();
        unlockedVersion.updateAuditInfo();
        return super.merge(unlockedVersion);
    }

    /** Uses the {@link AbstractCrud} method to persist an object. */
    public <V> void persistPojo(V pojo) {
        super.persist(pojo);
    }

    /**
     * String that can be used in exception messages so that the messages are properly customized to
     * the component class and CRUD class that are causing the exception.
     */
    public abstract String componentNameForExceptionMessages();
}
