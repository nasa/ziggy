package gov.nasa.ziggy.data.management;

import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.criterion.Restrictions;

import gov.nasa.ziggy.crud.AbstractCrud;

/**
 * CRUD class for {@link Manifest} instances.
 *
 * @author PT
 */
public class ManifestCrud extends AbstractCrud {

    /**
     * Determines whether a given dataset ID has already been used.
     */
    public boolean datasetIdExists(long datasetId) {
        Criteria criteria = createCriteria(Manifest.class);
        criteria.add(Restrictions.eq("datasetId", datasetId));
        List<Manifest> manifests = list(criteria);
        return !manifests.isEmpty();
    }

    /**
     * Retrieves the {@link Manifest} for a given pipeline task ID.
     */
    public Manifest retrieveByTaskId(long taskId) {
        Criteria criteria = createCriteria(Manifest.class);
        criteria.add(Restrictions.eq("importTaskId", taskId));
        return uniqueResult(criteria);
    }

}
