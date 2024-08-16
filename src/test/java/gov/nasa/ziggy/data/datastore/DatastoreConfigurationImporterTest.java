package gov.nasa.ziggy.data.datastore;

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
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableList;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import jakarta.xml.bind.JAXBException;

/**
 * Unit test class for {@link DatastoreConfigurationImporter}.
 *
 * @author PT
 */
public class DatastoreConfigurationImporterTest {

    private static final Path DATASTORE = TEST_DATA.resolve("datastore");
    private static final String FILE_1 = DATASTORE.resolve("pd-test-1.xml").toString();
    private static final String FILE_2 = DATASTORE.resolve("pd-test-2.xml").toString();
    private static final String NO_SUCH_FILE = "no-such-file.xml";
    private static final String NOT_REGULAR_FILE = TEST_DATA.resolve("configuration").toString();
    private static final String INVALID_FILE_1 = DATASTORE.resolve("pd-test-invalid-type.xml")
        .toString();
    private static final String INVALID_FILE_2 = DATASTORE.resolve("pd-test-invalid-xml")
        .toString();
    private static final String UPDATE_FILE = DATASTORE.resolve("datastore-update.xml").toString();

    private DatastoreOperations datastoreOperations = Mockito.mock(DatastoreOperations.class);

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Rule
    public ZiggyPropertyRule ziggyHomeDirPropertyRule = new ZiggyPropertyRule(ZIGGY_HOME_DIR,
        DirectoryProperties.ziggyCodeBuildDir().toString());

    // Basic functionality -- multiple files, multiple definitions, get imported
    @Test
    public void testBasicImport() throws JAXBException {

        DatastoreConfigurationImporter dataFileImporter = new DatastoreConfigurationImporter(
            ImmutableList.of(FILE_1, FILE_2), false);
        DatastoreConfigurationImporter importerSpy = Mockito.spy(dataFileImporter);
        setMocks(importerSpy);
        Mockito.when(datastoreOperations.datastoreNodesByFullPath()).thenReturn(new HashMap<>());
        Mockito.when(datastoreOperations.datastoreRegexpsByName()).thenReturn(new HashMap<>());
        importerSpy.importConfiguration();

        Set<DatastoreNode> nodesForDatabase = importerSpy.nodesForDatabase();
        assertEquals(10, nodesForDatabase.size());

        List<DatastoreRegexp> regexps = importerSpy.getRegexps();
        assertEquals(2, regexps.size());

        List<DataFileType> dataFileTypes = importerSpy.getDataFileTypes();
        assertEquals(3, dataFileTypes.size());
        Map<String, DataFileType> dataFileTypesByName = new HashMap<>();
        for (DataFileType dataFileType : dataFileTypes) {
            dataFileTypesByName.put(dataFileType.getName(), dataFileType);
        }
        validateDataFileTypes(dataFileTypesByName);

        assertEquals(2, importerSpy.getModelTypes().size());

        Map<String, DatastoreRegexp> databaseRegexps = importerSpy.regexpsByName();
        assertEquals(2, databaseRegexps.size());
        DatastoreRegexp regexp = databaseRegexps.get("cadenceType");
        assertNotNull(regexp);
        assertEquals("(target|ffi)", regexp.getValue());
        regexp = databaseRegexps.get("sector");
        assertNotNull(regexp);
        assertEquals("(sector-[0-9]{4})", regexp.getValue());

        Map<String, DatastoreNode> datastoreNodes = importerSpy.nodesByFullPath();
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
        Mockito.verify(datastoreOperations)
            .persistDatastoreConfiguration(ArgumentMatchers.<DataFileType> anyList(),
                ArgumentMatchers.<ModelType> anyList(),
                ArgumentMatchers.<DatastoreRegexp> anyList(),
                ArgumentMatchers.<DatastoreNode> anySet(),
                ArgumentMatchers.<DatastoreNode> anySet(), ArgumentMatchers.<Logger> any());
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
        assertEquals("sector/mda/dr/cal/pixels", dataFileType.getLocation());
        assertEquals("sector/mda/dr/cal/pixels/cadenceType/channel",
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
        DatastoreConfigurationImporter dataFileImporter = new DatastoreConfigurationImporter(
            ImmutableList.of(FILE_1, FILE_2), false);
        DatastoreConfigurationImporter importerSpy = Mockito.spy(dataFileImporter);
        setMocks(importerSpy);
        Mockito.when(datastoreOperations.datastoreNodesByFullPath()).thenReturn(new HashMap<>());
        Mockito.when(datastoreOperations.datastoreRegexpsByName()).thenReturn(new HashMap<>());
        importerSpy.importConfiguration();

        dataFileImporter = new DatastoreConfigurationImporter(List.of(UPDATE_FILE), false);
        DatastoreConfigurationImporter updaterSpy = Mockito.spy(dataFileImporter);
        setMocks(updaterSpy);

        // Use the initial imports to provide mocks for the DatastoreOperations methods.
        Map<String, DatastoreNode> importerSpyNodeMap = importerSpy.nodesByFullPath();

        // Note that the nodes that come out of the database into the update importer have
        // no XML nodes.
        for (DatastoreNode node : importerSpyNodeMap.values()) {
            node.getXmlNodes().clear();
        }
        Mockito.when(datastoreOperations.datastoreNodesByFullPath()).thenReturn(importerSpyNodeMap);
        Map<String, DatastoreRegexp> importerSpyRegexpMap = importerSpy.regexpsByName();
        Mockito.when(datastoreOperations.datastoreRegexpsByName()).thenReturn(importerSpyRegexpMap);
        List<String> regexpNames = new ArrayList<>(importerSpy.regexpsByName().keySet());
        Mockito.when(datastoreOperations.regexpNames()).thenReturn(regexpNames);
        List<String> dataFileTypeNames = importerSpy.getDataFileTypes()
            .stream()
            .map(DataFileType::getName)
            .collect(Collectors.toList());
        Mockito.when(datastoreOperations.dataFileTypeNames()).thenReturn(dataFileTypeNames);
        List<String> modelTypes = importerSpy.getModelTypes()
            .stream()
            .map(ModelType::getType)
            .collect(Collectors.toList());
        Mockito.when(datastoreOperations.modelTypes()).thenReturn(modelTypes);

        // Import the update file.
        updaterSpy.importConfiguration();

        assertTrue(updaterSpy.getDataFileTypes().isEmpty());
        assertTrue(updaterSpy.getModelTypes().isEmpty());

        List<String> updatedRegexpsNames = updaterSpy.getRegexps()
            .stream()
            .map(DatastoreRegexp::getName)
            .collect(Collectors.toList());
        assertTrue(updatedRegexpsNames.contains("cadenceType"));
        assertEquals(1, updatedRegexpsNames.size());

        List<String> removedNodeFullPaths = updaterSpy.getFullPathsForNodesToRemove();
        assertTrue(removedNodeFullPaths.contains("sector/mda/dr/pixels/cadenceType/channel"));
        assertTrue(removedNodeFullPaths.contains("sector/mda/cal"));
        assertTrue(removedNodeFullPaths.contains("sector/mda/cal/pixels"));
        assertTrue(removedNodeFullPaths.contains("sector/mda/cal/pixels/cadenceType"));
        assertTrue(removedNodeFullPaths.contains("sector/mda/cal/pixels/cadenceType/channel"));
        assertEquals(5, removedNodeFullPaths.size());

        List<String> nodesForDatabaseFullPaths = updaterSpy.nodesForDatabase()
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

    // Dry run test -- should import but not persist
    @Test
    public void testDryRun() throws JAXBException {

        DatastoreConfigurationImporter dataFileImporter = new DatastoreConfigurationImporter(
            ImmutableList.of(FILE_1, FILE_2), true);
        DatastoreConfigurationImporter importerSpy = Mockito.spy(dataFileImporter);
        setMocks(importerSpy);
        importerSpy.importConfiguration();

        Mockito.verify(datastoreOperations, Mockito.times(0))
            .persistDatastoreConfiguration(ArgumentMatchers.<DataFileType> anyList(),
                ArgumentMatchers.<ModelType> anyList(),
                ArgumentMatchers.<DatastoreRegexp> anyList(),
                ArgumentMatchers.<DatastoreNode> anySet(),
                ArgumentMatchers.<DatastoreNode> anySet(), ArgumentMatchers.<Logger> any());
    }

    @Test
    public void testDryRunOfUpdate() {
        DatastoreConfigurationImporter dataFileImporter = new DatastoreConfigurationImporter(
            ImmutableList.of(FILE_1, FILE_2), false);
        DatastoreConfigurationImporter importerSpy = Mockito.spy(dataFileImporter);
        setMocks(importerSpy);
        Mockito.when(datastoreOperations.datastoreNodesByFullPath()).thenReturn(new HashMap<>());
        Mockito.when(datastoreOperations.datastoreRegexpsByName()).thenReturn(new HashMap<>());
        importerSpy.importConfiguration();

        dataFileImporter = new DatastoreConfigurationImporter(List.of(UPDATE_FILE), true);
        DatastoreConfigurationImporter updaterSpy = Mockito.spy(dataFileImporter);
        setMocks(updaterSpy);

        // Use the initial imports to provide mocks for the DatastoreOperations methods.
        Map<String, DatastoreNode> importerSpyNodeMap = importerSpy.nodesByFullPath();

        // Note that the nodes that come out of the database into the update importer have
        // no XML nodes.
        for (DatastoreNode node : importerSpyNodeMap.values()) {
            node.getXmlNodes().clear();
        }
        Mockito.when(datastoreOperations.datastoreNodesByFullPath()).thenReturn(importerSpyNodeMap);
        Map<String, DatastoreRegexp> importerSpyRegexpMap = importerSpy.regexpsByName();
        Mockito.when(datastoreOperations.datastoreRegexpsByName()).thenReturn(importerSpyRegexpMap);
        List<String> regexpNames = new ArrayList<>(importerSpy.regexpsByName().keySet());
        Mockito.when(datastoreOperations.regexpNames()).thenReturn(regexpNames);
        List<String> dataFileTypeNames = importerSpy.getDataFileTypes()
            .stream()
            .map(DataFileType::getName)
            .collect(Collectors.toList());
        Mockito.when(datastoreOperations.dataFileTypeNames()).thenReturn(dataFileTypeNames);
        List<String> modelTypes = importerSpy.getModelTypes()
            .stream()
            .map(ModelType::getType)
            .collect(Collectors.toList());
        Mockito.when(datastoreOperations.modelTypes()).thenReturn(modelTypes);

        updaterSpy.importConfiguration();

        assertTrue(updaterSpy.getDataFileTypes().isEmpty());
        assertTrue(updaterSpy.getModelTypes().isEmpty());

        List<String> updatedRegexpsNames = updaterSpy.getRegexps()
            .stream()
            .map(DatastoreRegexp::getName)
            .collect(Collectors.toList());
        assertTrue(updatedRegexpsNames.contains("cadenceType"));
        assertEquals(1, updatedRegexpsNames.size());

        List<String> removedNodeFullPaths = updaterSpy.getFullPathsForNodesToRemove();
        assertTrue(removedNodeFullPaths.contains("sector/mda/dr/pixels/cadenceType/channel"));
        assertTrue(removedNodeFullPaths.contains("sector/mda/cal"));
        assertTrue(removedNodeFullPaths.contains("sector/mda/cal/pixels"));
        assertTrue(removedNodeFullPaths.contains("sector/mda/cal/pixels/cadenceType"));
        assertTrue(removedNodeFullPaths.contains("sector/mda/cal/pixels/cadenceType/channel"));
        assertEquals(5, removedNodeFullPaths.size());

        List<String> nodesForDatabaseFullPaths = updaterSpy.nodesForDatabase()
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

        // There should only be one call to the persist method in DatastoreOperations.
        Mockito.verify(datastoreOperations, Mockito.times(1))
            .persistDatastoreConfiguration(ArgumentMatchers.<DataFileType> anyList(),
                ArgumentMatchers.<ModelType> anyList(),
                ArgumentMatchers.<DatastoreRegexp> anyList(),
                ArgumentMatchers.<DatastoreNode> anySet(),
                ArgumentMatchers.<DatastoreNode> anySet(), ArgumentMatchers.<Logger> any());
    }

    // Test with missing and non-regular files -- should still import from the present,
    // regular files
    @Test
    public void testWithInvalidFiles() throws JAXBException {

        DatastoreConfigurationImporter dataFileImporter = new DatastoreConfigurationImporter(
            ImmutableList.of(FILE_1, FILE_2, NO_SUCH_FILE, NOT_REGULAR_FILE), false);
        DatastoreConfigurationImporter importerSpy = Mockito.spy(dataFileImporter);
        setMocks(importerSpy);
        importerSpy.importConfiguration();

        assertEquals(3, importerSpy.getDataFileTypes().size());
        Mockito.verify(datastoreOperations, Mockito.times(1))
            .persistDatastoreConfiguration(ArgumentMatchers.<DataFileType> anyList(),
                ArgumentMatchers.<ModelType> anyList(),
                ArgumentMatchers.<DatastoreRegexp> anyList(),
                ArgumentMatchers.<DatastoreNode> anySet(),
                ArgumentMatchers.<DatastoreNode> anySet(), ArgumentMatchers.<Logger> any());
    }

    // Test with a file that has an entry that is valid XML but instantiates to an
    // invalid DataFileType instance
    @Test
    public void testWithInvalidDataFileType() throws JAXBException {

        DatastoreConfigurationImporter dataFileImporter = new DatastoreConfigurationImporter(
            ImmutableList.of(FILE_1, INVALID_FILE_1), false);
        DatastoreConfigurationImporter importerSpy = Mockito.spy(dataFileImporter);
        setMocks(importerSpy);
        importerSpy.importConfiguration();

        assertEquals(2, importerSpy.getDataFileTypes().size());
        Mockito.verify(datastoreOperations, Mockito.times(1))
            .persistDatastoreConfiguration(ArgumentMatchers.<DataFileType> anyList(),
                ArgumentMatchers.<ModelType> anyList(),
                ArgumentMatchers.<DatastoreRegexp> anyList(),
                ArgumentMatchers.<DatastoreNode> anySet(),
                ArgumentMatchers.<DatastoreNode> anySet(), ArgumentMatchers.<Logger> any());
    }

    // Test with a file that has an entry that is invalid XML
    @Test
    public void testWithInvalidDataXml() throws JAXBException {

        DatastoreConfigurationImporter dataFileImporter = new DatastoreConfigurationImporter(
            ImmutableList.of(FILE_1, INVALID_FILE_2), false);
        DatastoreConfigurationImporter importerSpy = Mockito.spy(dataFileImporter);
        setMocks(importerSpy);
        importerSpy.importConfiguration();

        assertEquals(2, importerSpy.getDataFileTypes().size());
        Mockito.verify(datastoreOperations, Mockito.times(1))
            .persistDatastoreConfiguration(ArgumentMatchers.<DataFileType> anyList(),
                ArgumentMatchers.<ModelType> anyList(),
                ArgumentMatchers.<DatastoreRegexp> anyList(),
                ArgumentMatchers.<DatastoreNode> anySet(),
                ArgumentMatchers.<DatastoreNode> anySet(), ArgumentMatchers.<Logger> any());
    }

    @Test(expected = IllegalStateException.class)
    public void testDuplicateNames() throws JAXBException {

        DatastoreConfigurationImporter dataFileImporter = new DatastoreConfigurationImporter(
            ImmutableList.of(FILE_1, FILE_1), false);
        DatastoreConfigurationImporter importerSpy = Mockito.spy(dataFileImporter);
        setMocks(importerSpy);
        importerSpy.importConfiguration();
    }

    private void setMocks(DatastoreConfigurationImporter dataFileImporter) {
        Mockito.when(dataFileImporter.datastoreOperations()).thenReturn(datastoreOperations);
    }
}
