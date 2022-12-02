package gov.nasa.ziggy.pipeline.definition.crud;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.hibernate.Criteria;
import org.hibernate.criterion.Projections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.data.management.DataFileType;
import gov.nasa.ziggy.module.PipelineException;

/**
 * CRUD class for DataFileType.
 *
 * @author PT
 */
public class DataFileTypeCrud extends AbstractCrud {

    private static final Logger log = LoggerFactory.getLogger(DataFileTypeCrud.class);

    /**
     * Because the DataFileType encodes information about how each data file is stored in the
     * datastore, it is unsafe to update it -- it may cause the updated version to be unable to
     * locate the data that's already in the datastore. For this reason, we override the
     * createOrUpdate() method in AbstractCrud with one that simply throws an exception.
     */
    @Override
    public void createOrUpdate(Object o) {
        throw new PipelineException("Use of createOrUpdate in DataFileTypeCrud is forbidden");
    }

    /**
     * Because the DataFileType encodes information about how each data file is stored in the
     * datastore, it is unsafe to update it -- it may cause the updated version to be unable to
     * locate the data that's already in the datastore. For this reason, we override the update()
     * method in AbstractCrud with one that simply throws an exception.
     */
    @Override
    public void update(Object o) {
        throw new PipelineException("Use of update in DataFileTypeCrud is forbidden");
    }

    /**
     * Creates a collection of DataFileType objects. The collection is first searched for instances
     * that already exist in the database; these are ignored. The remaining, genuinely new instances
     * are then created. Note that any items in the collection that are not DataFileType instances
     * are also ignored.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void create(Collection<?> collection) {
        List<DataFileType> createdInstances = new ArrayList<>();
        List<String> existingNames = retrieveAllNames();
        if (existingNames == null) {
            existingNames = new ArrayList<>();
        }
        final List<String> finalExistingNames = existingNames;
        List<DataFileType> validInstances = (List<DataFileType>) collection.stream()
            .filter(s -> s instanceof DataFileType)
            .filter(s -> !finalExistingNames.contains(((DataFileType) s).getName()))
            .collect(Collectors.toList());
        if (validInstances.size() < collection.size()) {
            int invalidInstanceCount = collection.size() - validInstances.size();
            log.info("Removed " + invalidInstanceCount + " from collection of objects to create");
        }
        createdInstances.addAll(validInstances);
        log.info("Creating " + createdInstances.size() + " instances of DataFileType in database");
        super.create(createdInstances);
        log.info("Created " + createdInstances.size() + " instances of DataFileType in database");

    }

    /**
     * Retrieves the names of all existing DataFileType instances.
     */
    public List<String> retrieveAllNames() {
        Criteria criteria = createCriteria(DataFileType.class);
        criteria.setProjection(Projections.property("name"));
        return list(criteria);
    }

    /**
     * Retrieves all DataFileType instances in the database.
     */
    public List<DataFileType> retrieveAll() {
        Criteria criteria = createCriteria(DataFileType.class);
        return list(criteria);
    }

    /**
     * Retrieves a Map of all DataFileType objects, where the names are the keys and the
     * DataFileType instances the values.
     */
    public Map<String, DataFileType> retrieveMap() {
        List<DataFileType> dataFileTypes = retrieveAll();
        Map<String, DataFileType> dataFileTypeMap = new HashMap<>();
        for (DataFileType dataFileType : dataFileTypes) {
            dataFileTypeMap.put(dataFileType.getName(), dataFileType);
        }
        return dataFileTypeMap;
    }
}
