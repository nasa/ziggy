package gov.nasa.ziggy.data.datastore;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;

import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.database.DataFileTypeCrud;
import gov.nasa.ziggy.pipeline.definition.database.ModelCrud;
import gov.nasa.ziggy.services.database.DatabaseOperations;

/**
 * Provides database support for the datastore package. All datastore classes that require retrieval
 * from or persistence to the database must use methods of this class.
 *
 * @author PT
 */
public class DatastoreOperations extends DatabaseOperations {

    private DatastoreNodeCrud datastoreNodeCrud = new DatastoreNodeCrud();
    private DatastoreRegexpCrud datastoreRegexpCrud = new DatastoreRegexpCrud();
    private DataFileTypeCrud dataFileTypeCrud = new DataFileTypeCrud();
    private ModelCrud modelCrud = new ModelCrud();

    public Map<String, DatastoreNode> datastoreNodesByFullPath() {
        return performTransaction(() -> {
            Map<String, DatastoreNode> nodes = datastoreNodeCrud.retrieveNodesByFullPath();
            return nodes;
        });
    }

    public List<DatastoreRegexp> datastoreRegexps() {
        return performTransaction(() -> datastoreRegexpCrud.retrieveAll());
    }

    public Map<String, DatastoreRegexp> datastoreRegexpsByName() {
        return performTransaction(() -> datastoreRegexpCrud.retrieveRegexpsByName());
    }

    public List<String> regexpNames() {
        return performTransaction(() -> datastoreRegexpCrud.retrieveRegexpNames());
    }

    public List<String> dataFileTypeNames() {
        return performTransaction(() -> dataFileTypeCrud().retrieveAllNames());
    }

    public Map<String, DataFileType> dataFileTypeMap() {
        return performTransaction(() -> dataFileTypeCrud().retrieveMap());
    }

    public DataFileType dataFileType(String dataFileTypeName) {
        return performTransaction(() -> dataFileTypeCrud().retrieveByName(dataFileTypeName));
    }

    public List<String> modelTypes() {
        return performTransaction(() -> modelCrud.retrieveAllModelTypes()
            .stream()
            .map(ModelType::getType)
            .collect(Collectors.toList()));
    }

    public DatastoreRegexp datastoreRegexp(String regexpName) {
        return performTransaction(() -> datastoreRegexpCrud.retrieve(regexpName));
    }

    public void persistDatastoreConfiguration(List<DataFileType> dataFileTypes,
        List<ModelType> modelTypes, List<DatastoreRegexp> datastoreRegexps,
        Set<DatastoreNode> datastoreNodesToRemove, Set<DatastoreNode> datastoreNodes, Logger log) {
        performTransaction(() -> {
            log.info("Persisting to database {} DataFileType definitions", dataFileTypes.size());
            dataFileTypeCrud().persist(dataFileTypes);
            log.info("Persisting to database {} model definitions", modelTypes.size());
            modelCrud.persist(modelTypes);
            log.info("Persisting to database {} regexp definitions", datastoreRegexps.size());
            for (DatastoreRegexp regexp : datastoreRegexps) {
                datastoreRegexpCrud.merge(regexp);
            }
            log.info("Deleting from database {} datastore node definitions",
                datastoreNodesToRemove.size());
            for (DatastoreNode datastoreNodeToRemove : datastoreNodesToRemove) {
                datastoreNodeCrud.remove(datastoreNodeToRemove);
            }
            log.info("Persisting to database {} datastore node definitions", datastoreNodes.size());
            for (DatastoreNode nodeForDatabase : datastoreNodes) {
                datastoreNodeCrud.merge(nodeForDatabase);
            }
            log.info("Persist step complete");
        });
    }

    public DatastoreWalker newDatastoreWalkerInstance() {
        return performTransaction(() -> {
            Map<String, DatastoreRegexp> regexpsByName = datastoreRegexpCrud
                .retrieveRegexpsByName();
            Map<String, DatastoreNode> datastoreNodesByFullPath = datastoreNodeCrud
                .retrieveNodesByFullPath();
            return new DatastoreWalker(regexpsByName, datastoreNodesByFullPath);
        });
    }

    public DatastoreRegexp mergeDatastoreRegexp(DatastoreRegexp regexp) {
        return performTransaction(() -> datastoreRegexpCrud.merge(regexp));
    }

    DataFileTypeCrud dataFileTypeCrud() {
        return dataFileTypeCrud;
    }
}
