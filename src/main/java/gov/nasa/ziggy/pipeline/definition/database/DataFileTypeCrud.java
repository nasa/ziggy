package gov.nasa.ziggy.pipeline.definition.database;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.data.datastore.DataFileType_;
import gov.nasa.ziggy.module.PipelineException;

/**
 * CRUD class for DataFileType.
 *
 * @author PT
 */
public class DataFileTypeCrud extends AbstractCrud<DataFileType> {

    private static final Logger log = LoggerFactory.getLogger(DataFileTypeCrud.class);

    /**
     * Because the DataFileType encodes information about how each data file is stored in the
     * datastore, it is unsafe to update it -- it may cause the updated version to be unable to
     * locate the data that's already in the datastore. For this reason, we override the merge()
     * method in AbstractCrud with one that simply throws an exception.
     */
    @Override
    public <T> T merge(T o) {
        throw new PipelineException("Use of merge in DataFileTypeCrud is forbidden");
    }

    /**
     * Creates a collection of DataFileType objects. The collection is first searched for instances
     * that already exist in the database; these are ignored. The remaining, genuinely new instances
     * are then created. Note that any items in the collection that are not DataFileType instances
     * are also ignored.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void persist(Collection<?> collection) {
        List<DataFileType> createdInstances = new ArrayList<>();
        List<String> existingNames = retrieveAllNames();
        if (existingNames == null) {
            existingNames = new ArrayList<>();
        }
        final List<String> finalExistingNames = existingNames;
        List<DataFileType> validInstances = (List<DataFileType>) collection.stream()
            .filter(DataFileType.class::isInstance)
            .filter(s -> !finalExistingNames.contains(((DataFileType) s).getName()))
            .collect(Collectors.toList());
        if (validInstances.size() < collection.size()) {
            int invalidInstanceCount = collection.size() - validInstances.size();
            log.info("Removed {} from collection of objects to create", invalidInstanceCount);
        }
        createdInstances.addAll(validInstances);
        log.info("Creating {} instances of DataFileType in database", createdInstances.size());
        super.persist(createdInstances);
        log.info("Created {} instances of DataFileType in database", createdInstances.size());
    }

    public DataFileType retrieveByName(String name) {
        ZiggyQuery<DataFileType, DataFileType> query = createZiggyQuery(DataFileType.class);
        query.column(DataFileType_.name).in(name);
        return uniqueResult(query);
    }

    /**
     * Retrieves the names of all existing DataFileType instances.
     */
    public List<String> retrieveAllNames() {
        ZiggyQuery<DataFileType, String> query = createZiggyQuery(DataFileType.class, String.class);
        query.column(DataFileType_.name).select();
        return list(query);
    }

    /**
     * Retrieves all DataFileType instances in the database.
     */
    public List<DataFileType> retrieveAll() {
        return list(createZiggyQuery(DataFileType.class));
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

    @Override
    public Class<DataFileType> componentClass() {
        return DataFileType.class;
    }
}
