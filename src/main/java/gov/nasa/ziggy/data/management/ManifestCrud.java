package gov.nasa.ziggy.data.management;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.crud.ZiggyQuery;

/**
 * CRUD class for {@link Manifest} instances.
 *
 * @author PT
 */
public class ManifestCrud extends AbstractCrud<Manifest> {

    /**
     * Determines whether a given dataset ID has already been used.
     */
    public boolean datasetIdExists(long datasetId) {
        ZiggyQuery<Manifest, Manifest> query = createZiggyQuery(Manifest.class);
        query.column(Manifest_.datasetId).in(datasetId);
        return !list(query).isEmpty();
    }

    /**
     * Retrieves the {@link Manifest} for a given pipeline task ID.
     */
    public Manifest retrieveByTaskId(long taskId) {
        ZiggyQuery<Manifest, Manifest> query = createZiggyQuery(Manifest.class);
        return uniqueResult(query.column(Manifest_.importTaskId).in(taskId));
    }

    @Override
    public Class<Manifest> componentClass() {
        return Manifest.class;
    }
}
