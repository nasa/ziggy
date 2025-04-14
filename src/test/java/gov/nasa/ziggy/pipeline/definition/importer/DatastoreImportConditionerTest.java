package gov.nasa.ziggy.pipeline.definition.importer;

import static gov.nasa.ziggy.ZiggyUnitTestUtils.TEST_DATA;
import static gov.nasa.ziggy.services.config.PropertyName.ZIGGY_HOME_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.data.datastore.DatastoreNode;
import gov.nasa.ziggy.data.datastore.DatastoreOperations;
import gov.nasa.ziggy.data.datastore.DatastoreRegexp;
import gov.nasa.ziggy.data.datastore.DatastoreWalker;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;
import jakarta.xml.bind.JAXBException;

/**
 * Unit test class for {@link DatastoreImportConditioner}.
 *
 * @author PT
 */
public class DatastoreImportConditionerTest {

    private static final Path FILE_1 = TEST_DATA.resolve("pd-test-1.xml");
    private static final Path FILE_2 = TEST_DATA.resolve("pd-test-2.xml");
    private static final Path INVALID_FILE_1 = TEST_DATA.resolve("pd-test-invalid-type.xml");
    private static final Path UPDATE_FILE = TEST_DATA.resolve("datastore-update.xml");

    private DatastoreOperations datastoreOperations = Mockito.mock(DatastoreOperations.class);

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Rule
    public ZiggyPropertyRule ziggyHomeDirPropertyRule = new ZiggyPropertyRule(ZIGGY_HOME_DIR,
        DirectoryProperties.ziggyCodeBuildDir().toString());

    @Rule
    public ZiggyPropertyRule ziggyDatastoreRootRule = new ZiggyPropertyRule(
        PropertyName.DATASTORE_ROOT_DIR, "/dev/null");

    // Basic functionality -- multiple files, multiple definitions, get imported
    @Test
    public void testBasicImport() throws JAXBException {

        PipelineDefinitionImporter pipelineDefinitionImporter = new PipelineDefinitionImporter(
            ImmutableList.of(FILE_1, FILE_2));
        DatastoreImportConditioner conditioner = pipelineDefinitionImporter
            .datastoreImportConditioner();
        DatastoreImportConditioner conditionerSpy = Mockito.spy(conditioner);
        setMocks(conditionerSpy);
        Mockito.when(datastoreOperations.datastoreNodesByFullPath()).thenReturn(new HashMap<>());
        Mockito.when(datastoreOperations.datastoreRegexpsByName()).thenReturn(new HashMap<>());
        conditionerSpy.checkDefinitions();

        Set<DatastoreNode> nodesForDatabase = conditionerSpy.nodesForDatabase();
        assertEquals(10, nodesForDatabase.size());

        List<DatastoreRegexp> regexps = conditionerSpy.getRegexps();
        assertEquals(2, regexps.size());

        List<DataFileType> dataFileTypes = conditionerSpy.getDataFileTypes();
        assertEquals(3, dataFileTypes.size());
        Map<String, DataFileType> dataFileTypesByName = new HashMap<>();
        for (DataFileType dataFileType : dataFileTypes) {
            dataFileTypesByName.put(dataFileType.getName(), dataFileType);
        }
        validateDataFileTypes(dataFileTypesByName);

        assertEquals(2, conditionerSpy.getModelTypes().size());

        Map<String, DatastoreRegexp> databaseRegexps = conditionerSpy.regexpsByName();
        assertEquals(2, databaseRegexps.size());
        DatastoreRegexp regexp = databaseRegexps.get("cadenceType");
        assertNotNull(regexp);
        assertEquals("(target|ffi)", regexp.getValue());
        regexp = databaseRegexps.get("sector");
        assertNotNull(regexp);
        assertEquals("(sector-[0-9]{4})", regexp.getValue());

        Map<String, DatastoreNode> datastoreNodes = conditionerSpy.nodesByFullPath();
        DatastoreNode sectorNode = testNode(datastoreNodes, "sector", true, 1, null);
        DatastoreNode mdaNode = testNode(datastoreNodes, "mda", false, 2, sectorNode);

        // DR nodes
        DatastoreNode drNode = testNode(datastoreNodes, "dr", false, 1, mdaNode);
        DatastoreNode drPixelNode = testNode(datastoreNodes, "pixels", false, 1, drNode);
        DatastoreNode drCadenceTypeNode = testNode(datastoreNodes, "cadenceType", true, 1,
            drPixelNode);
        testNode(datastoreNodes, "channel", false, 0, drCadenceTypeNode);

        // CAL nodes
        DatastoreNode calNode = testNode(datastoreNodes, "cal", false, 1, mdaNode);
        DatastoreNode calPixelNode = testNode(datastoreNodes, "pixels", false, 1, calNode);
        DatastoreNode calCadenceTypeNode = testNode(datastoreNodes, "cadenceType", true, 1,
            calPixelNode);
        testNode(datastoreNodes, "channel", false, 0, calCadenceTypeNode);

        assertEquals(10, datastoreNodes.size());
    }

    private void validateDataFileTypes(Map<String, DataFileType> dataFileTypes) {

        DataFileType dataFileType = dataFileTypes.get("calibrated pixels");
        assertNotNull(dataFileType);
        assertEquals("[1-4]:[1-4]:[A-D]-calibrated-pixels.h5", dataFileType.getFileNameRegexp());
        assertEquals("sector/mda/cal/pixels/cadenceType/channel", dataFileType.getLocation());
        assertEquals("sector/mda/cal/pixels/cadenceType/channel",
            DatastoreWalker.fullLocation(dataFileType));
        assertFalse(dataFileType.isIncludeAllFilesInAllSubtasks());

        dataFileType = dataFileTypes.get("raw flux");
        assertNotNull(dataFileType);
        assertEquals("[1-4]:[1-4]:[A-D]-raw-pixels.h5", dataFileType.getFileNameRegexp());
        assertEquals("sector/mda/dr/pixels/cadenceType/channel", dataFileType.getLocation());
        assertEquals("sector/mda/dr/pixels/cadenceType/channel",
            DatastoreWalker.fullLocation(dataFileType));
        assertFalse(dataFileType.isIncludeAllFilesInAllSubtasks());

        dataFileType = dataFileTypes.get("TCE");
        assertNotNull(dataFileType);
        assertEquals("tce.h5", DatastoreWalker.fileNameRegexpBaseName(dataFileType));
        assertEquals("sector/mda/cal/pixels", dataFileType.getLocation());
        assertEquals("sector/mda/cal/pixels/cadenceType/channel",
            DatastoreWalker.fullLocation(dataFileType));
        assertTrue(dataFileType.isIncludeAllFilesInAllSubtasks());
    }

    private DatastoreNode testNode(Map<String, DatastoreNode> datastoreNodes, String name,
        boolean regexp, int expectedChildNodeCount, DatastoreNode parentNode) {
        String parentFullPath = parentNode != null ? parentNode.getFullPath() : "";
        String fullPath = DatastoreWalker.fullPathFromParentPath(name, parentFullPath);
        DatastoreNode node = datastoreNodes.get(fullPath);
        assertNotNull(node);
        assertEquals(name, node.getName());
        assertEquals(regexp, node.isRegexp());
        assertEquals(expectedChildNodeCount, node.getChildNodeFullPaths().size());
        if (parentNode != null) {
            assertTrue(parentNode.getChildNodeFullPaths().contains(fullPath));
        }
        return node;
    }

    @Test
    public void testUpdateDatastore() {
        PipelineDefinitionImporter pipelineDefinitionImporter = new PipelineDefinitionImporter(
            ImmutableList.of(FILE_1, FILE_2));
        DatastoreImportConditioner conditioner = pipelineDefinitionImporter
            .datastoreImportConditioner();
        DatastoreImportConditioner conditionerSpy = Mockito.spy(conditioner);
        setMocks(conditionerSpy);
        Mockito.when(datastoreOperations.datastoreNodesByFullPath()).thenReturn(new HashMap<>());
        Mockito.when(datastoreOperations.datastoreRegexpsByName()).thenReturn(new HashMap<>());
        conditionerSpy.checkDefinitions();

        // Use the initial imports to provide mocks for the DatastoreOperations methods.
        Map<String, DatastoreNode> importerSpyNodeMap = conditionerSpy.nodesByFullPath();
        Map<String, DatastoreRegexp> importerSpyRegexpMap = conditionerSpy.regexpsByName();
        List<String> regexpNames = new ArrayList<>(conditionerSpy.regexpsByName().keySet());

        // Note that the nodes that come out of the database into the update importer have
        // no XML nodes.
        for (DatastoreNode node : importerSpyNodeMap.values()) {
            node.getXmlNodes().clear();
        }

        pipelineDefinitionImporter = new PipelineDefinitionImporter(ImmutableList.of(UPDATE_FILE));
        conditioner = pipelineDefinitionImporter.datastoreImportConditioner();
        conditionerSpy = Mockito.spy(conditioner);
        setMocks(conditionerSpy);

        Mockito.when(datastoreOperations.datastoreNodesByFullPath()).thenReturn(importerSpyNodeMap);
        Mockito.when(datastoreOperations.datastoreRegexpsByName()).thenReturn(importerSpyRegexpMap);
        Mockito.when(datastoreOperations.regexpNames()).thenReturn(regexpNames);
        List<String> dataFileTypeNames = conditionerSpy.getDataFileTypes()
            .stream()
            .map(DataFileType::getName)
            .collect(Collectors.toList());
        Mockito.when(datastoreOperations.dataFileTypeNames()).thenReturn(dataFileTypeNames);
        List<String> modelTypes = conditionerSpy.getModelTypes()
            .stream()
            .map(ModelType::getType)
            .collect(Collectors.toList());
        Mockito.when(datastoreOperations.modelTypes()).thenReturn(modelTypes);

        // Import the update file.
        conditionerSpy.checkDefinitions();

        assertTrue(conditionerSpy.getDataFileTypes().isEmpty());
        assertTrue(conditionerSpy.getModelTypes().isEmpty());

        List<String> updatedRegexpsNames = conditionerSpy.getRegexps()
            .stream()
            .map(DatastoreRegexp::getName)
            .collect(Collectors.toList());
        assertTrue(updatedRegexpsNames.contains("cadenceType"));
        assertEquals(1, updatedRegexpsNames.size());

        List<String> removedNodeFullPaths = conditionerSpy.getFullPathsForNodesToRemove();
        assertTrue(removedNodeFullPaths.contains("sector/mda/dr/pixels/cadenceType/channel"));
        assertTrue(removedNodeFullPaths.contains("sector/mda/cal"));
        assertTrue(removedNodeFullPaths.contains("sector/mda/cal/pixels"));
        assertTrue(removedNodeFullPaths.contains("sector/mda/cal/pixels/cadenceType"));
        assertTrue(removedNodeFullPaths.contains("sector/mda/cal/pixels/cadenceType/channel"));
        assertEquals(5, removedNodeFullPaths.size());

        List<String> nodesForDatabaseFullPaths = conditionerSpy.nodesForDatabase()
            .stream()
            .map(DatastoreNode::getFullPath)
            .collect(Collectors.toList());
        assertTrue(nodesForDatabaseFullPaths.contains("sector/mda/pa"));
        assertTrue(nodesForDatabaseFullPaths.contains("sector/mda/pa/raw-flux"));
        assertTrue(nodesForDatabaseFullPaths.contains("sector/mda/pa/raw-flux/cadenceType"));
        assertTrue(nodesForDatabaseFullPaths.contains("sector/mda/pa/raw-flux/cadenceType/ccd"));
        assertTrue(nodesForDatabaseFullPaths.contains("sector"));
        assertTrue(nodesForDatabaseFullPaths.contains("sector/mda"));
        assertTrue(nodesForDatabaseFullPaths.contains("sector/mda/dr"));
        assertTrue(nodesForDatabaseFullPaths.contains("sector/mda/dr/pixels"));
        assertTrue(nodesForDatabaseFullPaths.contains("sector/mda/dr/pixels/cadenceType"));
        assertTrue(nodesForDatabaseFullPaths.contains("sector/mda/dr/pixels/cadenceType/ccd"));
        assertEquals(10, nodesForDatabaseFullPaths.size());
    }

    // Test with a file that has an entry that is valid XML but instantiates to an
    // invalid DataFileType instance
    @Test
    public void testWithInvalidDataFileType() throws JAXBException {

        PipelineDefinitionImporter pipelineDefinitionImporter = new PipelineDefinitionImporter(
            ImmutableList.of(FILE_1, INVALID_FILE_1));
        DatastoreImportConditioner conditioner = pipelineDefinitionImporter
            .datastoreImportConditioner();
        DatastoreImportConditioner conditionerSpy = Mockito.spy(conditioner);
        setMocks(conditionerSpy);
        conditionerSpy.checkDefinitions();

        assertEquals(2, conditionerSpy.getDataFileTypes().size());
    }

    @Test(expected = IllegalStateException.class)
    public void testDuplicateNames() throws JAXBException {

        PipelineDefinitionImporter pipelineDefinitionImporter = new PipelineDefinitionImporter(
            ImmutableList.of(FILE_1, FILE_1));
        DatastoreImportConditioner conditioner = pipelineDefinitionImporter
            .datastoreImportConditioner();
        DatastoreImportConditioner conditionerSpy = Mockito.spy(conditioner);
        setMocks(conditionerSpy);
        conditionerSpy.checkDefinitions();
    }

    private void setMocks(DatastoreImportConditioner dataFileImporter) {
        Mockito.when(dataFileImporter.datastoreOperations()).thenReturn(datastoreOperations);
    }
}
