package gov.nasa.ziggy.data.datastore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.database.DataFileTypeCrud;
import gov.nasa.ziggy.pipeline.definition.database.ModelCrud;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.database.DatabaseOperations;

/**
 * Unit tests for {@link DatastoreOperations} class.
 *
 * @author PT
 */
public class DatastoreOperationsTest {

    private static final Logger log = LoggerFactory.getLogger(DatastoreOperationsTest.class);

    @Rule
    public final ZiggyDatabaseRule ziggyDatabaseRule = new ZiggyDatabaseRule();

    @Rule
    public final ZiggyPropertyRule propertyRule = new ZiggyPropertyRule(
        PropertyName.DATASTORE_ROOT_DIR, "");

    @Test
    public void testDatastoreNodesByFullPath() {
        DatastoreNode node1 = new DatastoreNode("parent", false);
        node1.setFullPath("parent");
        DatastoreNode node2 = new DatastoreNode("child1", false);
        node2.setFullPath("parent/child1");
        DatastoreNode node3 = new DatastoreNode("child2", false);
        node3.setFullPath("parent/child2");
        DatastoreNode node4 = new DatastoreNode("grandchild", false);
        node4.setFullPath("parent/child1/grandchild");
        node1.setChildNodeFullPaths(List.of(node2.getFullPath(), node3.getFullPath()));
        node2.setChildNodeFullPaths(List.of(node4.getFullPath()));
        new TestOperations().persistNodes(List.of(node1, node2, node3, node4));
        Map<String, DatastoreNode> datastoreNodesByFullPath = new DatastoreOperations()
            .datastoreNodesByFullPath();
        Set<String> fullPaths = datastoreNodesByFullPath.keySet();
        assertTrue(fullPaths.contains(node1.getFullPath()));
        assertEquals(node1, datastoreNodesByFullPath.get(node1.getFullPath()));
        List<String> childNodeFullPaths = node1.getChildNodeFullPaths();
        assertTrue(childNodeFullPaths.contains(node2.getFullPath()));
        assertTrue(childNodeFullPaths.contains(node3.getFullPath()));
        assertEquals(2, childNodeFullPaths.size());
        assertTrue(fullPaths.contains(node2.getFullPath()));
        assertEquals(node2, datastoreNodesByFullPath.get(node2.getFullPath()));
        childNodeFullPaths = node2.getChildNodeFullPaths();
        assertTrue(childNodeFullPaths.contains(node4.getFullPath()));
        assertEquals(1, childNodeFullPaths.size());
        assertTrue(fullPaths.contains(node3.getFullPath()));
        assertEquals(node3, datastoreNodesByFullPath.get(node3.getFullPath()));
        assertTrue(node3.getChildNodeFullPaths().isEmpty());
        assertTrue(fullPaths.contains(node4.getFullPath()));
        assertEquals(node4, datastoreNodesByFullPath.get(node4.getFullPath()));
        assertTrue(node4.getChildNodeFullPaths().isEmpty());
        assertEquals(4, datastoreNodesByFullPath.size());
    }

    @Test
    public void testDatastoreRegexpsByName() {
        DatastoreRegexp regexp1 = new DatastoreRegexp("r1", "\\S+");
        DatastoreRegexp regexp2 = new DatastoreRegexp("r2", "[0-9]+");
        new TestOperations().persistRegexps(List.of(regexp1, regexp2));
        Map<String, DatastoreRegexp> regexpsByName = new DatastoreOperations()
            .datastoreRegexpsByName();
        Set<String> regexpNames = regexpsByName.keySet();
        assertTrue(regexpNames.contains(regexp1.getName()));
        assertEquals(regexp1, regexpsByName.get(regexp1.getName()));
        assertEquals(regexp1.getValue(), regexpsByName.get(regexp1.getName()).getValue());
        assertTrue(regexpNames.contains(regexp2.getName()));
        assertEquals(regexp2, regexpsByName.get(regexp2.getName()));
        assertEquals(regexp2.getValue(), regexpsByName.get(regexp2.getName()).getValue());
        assertEquals(2, regexpsByName.size());
    }

    @Test
    public void testRegexpNames() {
        DatastoreRegexp regexp1 = new DatastoreRegexp("r1", "\\S+");
        DatastoreRegexp regexp2 = new DatastoreRegexp("r2", "[0-9]+");
        new TestOperations().persistRegexps(List.of(regexp1, regexp2));
        List<String> regexpNames = new DatastoreOperations().regexpNames();
        assertTrue(regexpNames.contains(regexp1.getName()));
        assertTrue(regexpNames.contains(regexp2.getName()));
        assertEquals(2, regexpNames.size());
    }

    @Test
    public void testDataFileTypeNames() {
        DataFileType d1 = new DataFileType("d1", "dummy", "dummy");
        DataFileType d2 = new DataFileType("d2", "dummy/dummy", "dummy");
        new TestOperations().persistDataFileTypes(List.of(d1, d2));
        List<String> dataFileTypeNames = new DatastoreOperations().dataFileTypeNames();
        assertTrue(dataFileTypeNames.contains(d1.getName()));
        assertTrue(dataFileTypeNames.contains(d2.getName()));
        assertEquals(2, dataFileTypeNames.size());
    }

    @Test
    public void testDataFileTypeMap() {
        DataFileType d1 = new DataFileType("d1", "dummy", "dummy");
        DataFileType d2 = new DataFileType("d2", "dummy/dummy", "dummy");
        new TestOperations().persistDataFileTypes(List.of(d1, d2));
        Map<String, DataFileType> dataFileTypesByName = new DatastoreOperations().dataFileTypeMap();
        Set<String> dataFileTypeNames = dataFileTypesByName.keySet();
        assertTrue(dataFileTypeNames.contains(d1.getName()));
        assertEquals(d1, dataFileTypesByName.get(d1.getName()));
        assertTrue(dataFileTypeNames.contains(d2.getName()));
        assertEquals(d2, dataFileTypesByName.get(d2.getName()));
        assertEquals(2, dataFileTypesByName.size());
    }

    @Test
    public void testDataFileTypeRetrieval() {
        DataFileType d1 = new DataFileType("d1", "dummy", "dummy");
        DataFileType d2 = new DataFileType("d2", "dummy/dummy", "dummy");
        new TestOperations().persistDataFileTypes(List.of(d1, d2));
        DataFileType fromDatabase = new DatastoreOperations().dataFileType("d1");
        assertEquals("d1", fromDatabase.getName());
        assertEquals("dummy", fromDatabase.getLocation());
        assertEquals("dummy", fromDatabase.getFileNameRegexp());
    }

    @Test
    public void testModelTypes() {
        ModelType m1 = new ModelType();
        m1.setType("m1");
        ModelType m2 = new ModelType();
        m2.setType("m2");
        new TestOperations().persistModelTypes(List.of(m1, m2));
        List<String> modelTypes = new DatastoreOperations().modelTypes();
        assertTrue(modelTypes.contains(m1.getType()));
        assertTrue(modelTypes.contains(m2.getType()));
        assertEquals(2, modelTypes.size());
    }

    @Test
    public void testNewDatastoreWalkerInstance() {
        DatastoreNode node1 = new DatastoreNode("parent", false);
        node1.setFullPath("parent");
        DatastoreNode node2 = new DatastoreNode("child1", false);
        node2.setFullPath("parent/child1");
        DatastoreNode node3 = new DatastoreNode("child2", false);
        node3.setFullPath("parent/child2");
        DatastoreNode node4 = new DatastoreNode("grandchild", false);
        node4.setFullPath("parent/child1/grandchild");
        node1.setChildNodeFullPaths(List.of(node2.getFullPath(), node3.getFullPath()));
        node2.setChildNodeFullPaths(List.of(node4.getFullPath()));
        DatastoreRegexp regexp1 = new DatastoreRegexp("r1", "\\S+");
        DatastoreRegexp regexp2 = new DatastoreRegexp("r2", "[0-9]+");
        new TestOperations().persistRegexps(List.of(regexp1, regexp2));
        new TestOperations().persistNodes(List.of(node1, node2, node3, node4));
        DatastoreWalker datastoreWalker = new DatastoreOperations().newDatastoreWalkerInstance();
        assertEquals(new DatastoreOperations().datastoreRegexpsByName(),
            datastoreWalker.regexpsByName());
        assertEquals(new DatastoreOperations().datastoreNodesByFullPath(),
            datastoreWalker.datastoreNodesByFullPath());
    }

    @Test
    public void testPersistDatastoreConfiguration() {
        DatastoreNode node1 = new DatastoreNode("parent", false);
        node1.setFullPath("parent");
        DatastoreNode node2 = new DatastoreNode("child1", false);
        node2.setFullPath("parent/child1");
        DatastoreNode node3 = new DatastoreNode("child2", false);
        node3.setFullPath("parent/child2");
        DatastoreNode node4 = new DatastoreNode("grandchild", false);
        node4.setFullPath("parent/child1/grandchild");
        node1.setChildNodeFullPaths(List.of(node2.getFullPath(), node3.getFullPath()));
        node2.setChildNodeFullPaths(List.of(node4.getFullPath()));
        Set<DatastoreNode> nodesToAdd = Set.of(node1, node2, node3, node4);

        DatastoreRegexp regexp1 = new DatastoreRegexp("r1", "\\S+");
        DatastoreRegexp regexp2 = new DatastoreRegexp("r2", "[0-9]+");
        List<DatastoreRegexp> datastoreRegexpsToAdd = List.of(regexp1, regexp2);

        ModelType m1 = new ModelType();
        m1.setType("m1");
        ModelType m2 = new ModelType();
        m2.setType("m2");
        List<ModelType> modelsToAdd = List.of(m1, m2);

        DataFileType d1 = new DataFileType("d1", "dummy", "dummy");
        DataFileType d2 = new DataFileType("d2", "dummy/dummy", "dummy");
        List<DataFileType> dataFileTypesToAdd = List.of(d1, d2);

        Set<DatastoreNode> nodesToRemove = new TestOperations().nodesToRemove();

        new DatastoreOperations().persistDatastoreConfiguration(dataFileTypesToAdd, modelsToAdd,
            datastoreRegexpsToAdd, nodesToRemove, nodesToAdd, log);

        Map<String, DatastoreNode> datastoreNodesByFullPath = new DatastoreOperations()
            .datastoreNodesByFullPath();
        Collection<DatastoreNode> datastoreNodes = datastoreNodesByFullPath.values();
        assertTrue(datastoreNodes.contains(node1));
        assertTrue(datastoreNodes.contains(node2));
        assertTrue(datastoreNodes.contains(node3));
        assertTrue(datastoreNodes.contains(node4));
        assertEquals(4, datastoreNodes.size());

        Map<String, DatastoreRegexp> regexpsByName = new DatastoreOperations()
            .datastoreRegexpsByName();
        Collection<DatastoreRegexp> regexps = regexpsByName.values();
        assertTrue(regexps.contains(regexp1));
        assertTrue(regexps.contains(regexp2));
        assertEquals(2, regexps.size());

        List<String> modelTypes = new DatastoreOperations().modelTypes();
        assertTrue(modelTypes.contains(m1.getType()));
        assertTrue(modelTypes.contains(m2.getType()));
        assertEquals(2, modelTypes.size());

        Map<String, DataFileType> dataFileTypeMap = new DatastoreOperations().dataFileTypeMap();
        Collection<DataFileType> dataFileTypes = dataFileTypeMap.values();
        assertTrue(dataFileTypes.contains(d1));
        assertTrue(dataFileTypes.contains(d2));
        assertEquals(2, dataFileTypes.size());
    }

    @Test
    public void testDatastoreRegexp() {
        DatastoreRegexp regexp1 = new DatastoreRegexp("r1", "\\S+");
        DatastoreRegexp regexp2 = new DatastoreRegexp("r2", "[0-9]+");
        new TestOperations().persistRegexps(List.of(regexp1, regexp2));
        DatastoreRegexp datastoreRegexp = new DatastoreOperations().datastoreRegexp("r1");
        assertEquals("\\S+", datastoreRegexp.getValue());
    }

    private static class TestOperations extends DatabaseOperations {

        public void persistNodes(List<DatastoreNode> datastoreNodes) {
            performTransaction(() -> new DatastoreNodeCrud().persist(datastoreNodes));
        }

        public void persistRegexps(List<DatastoreRegexp> datastoreRegexps) {
            performTransaction(() -> new DatastoreRegexpCrud().persist(datastoreRegexps));
        }

        public void persistDataFileTypes(List<DataFileType> dataFileTypes) {
            performTransaction(() -> new DataFileTypeCrud().persist(dataFileTypes));
        }

        public void persistModelTypes(List<ModelType> modelTypes) {
            performTransaction(() -> new ModelCrud().persist(modelTypes));
        }

        public Set<DatastoreNode> nodesToRemove() {
            return performTransaction(() -> {
                DatastoreNode node5 = new DatastoreNode("parent/node5", false);
                node5.setFullPath("node5");
                DatastoreNode node6 = new DatastoreNode("node6", false);
                node6.setFullPath("parent/node6");
                new DatastoreNodeCrud().persist(List.of(node5, node6));
                return Set.of(node5, node6);
            });
        }
    }
}
