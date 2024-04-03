package gov.nasa.ziggy.data.datastore;

import static gov.nasa.ziggy.ZiggyUnitTestUtils.TEST_DATA;
import static gov.nasa.ziggy.services.config.PropertyName.ZIGGY_HOME_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hibernate.Hibernate;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.crud.DataFileTypeCrud;
import gov.nasa.ziggy.pipeline.definition.crud.ModelCrud;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
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

    private DataFileTypeCrud dataFileTypeCrud = Mockito.spy(DataFileTypeCrud.class);
    private ModelCrud modelCrud = Mockito.spy(ModelCrud.class);
    private DatastoreNodeCrud nodeCrud = Mockito.spy(DatastoreNodeCrud.class);
    private DatastoreRegexpCrud regexpCrud = Mockito.spy(DatastoreRegexpCrud.class);

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Rule
    public ZiggyPropertyRule ziggyHomeDirPropertyRule = new ZiggyPropertyRule(ZIGGY_HOME_DIR,
        DirectoryProperties.ziggyCodeBuildDir().toString());

    // Basic functionality -- multiple files, multiple definitions, get imported
    @SuppressWarnings("unchecked")
    @Test
    public void testBasicImport() throws JAXBException {

        DatastoreConfigurationImporter dataFileImporter = new DatastoreConfigurationImporter(
            ImmutableList.of(FILE_1, FILE_2), false);
        DatastoreConfigurationImporter importerSpy = Mockito.spy(dataFileImporter);
        setMocks(importerSpy);
        DatabaseTransactionFactory.performTransaction(() -> {
            importerSpy.importConfiguration();
            return null;
        });

        Set<DatastoreNode> nodesForDatabase = importerSpy.nodesForDatabase();
        assertEquals(10, nodesForDatabase.size());

        List<DatastoreRegexp> regexps = importerSpy.getRegexps();
        assertEquals(2, regexps.size());

        assertEquals(3, importerSpy.getDataFileTypes().size());
        Mockito.verify(dataFileTypeCrud, Mockito.times(1))
            .persist(ArgumentMatchers.<DataFileType> anyList());

        assertEquals(2, importerSpy.getModelTypes().size());
        Mockito.verify(modelCrud, Mockito.times(1)).persist(ArgumentMatchers.<ModelType> anyList());

        Map<String, DatastoreRegexp> databaseRegexps = (Map<String, DatastoreRegexp>) DatabaseTransactionFactory
            .performTransaction(() -> regexpCrud.retrieveRegexpsByName());
        assertEquals(2, databaseRegexps.size());
        DatastoreRegexp regexp = databaseRegexps.get("cadenceType");
        assertNotNull(regexp);
        assertEquals("(target|ffi)", regexp.getValue());
        regexp = databaseRegexps.get("sector");
        assertNotNull(regexp);
        assertEquals("(sector-[0-9]{4})", regexp.getValue());

        Map<String, DatastoreNode> datastoreNodes = (Map<String, DatastoreNode>) DatabaseTransactionFactory
            .performTransaction(() -> {
                Map<String, DatastoreNode> nodes = nodeCrud.retrieveNodesByFullPath();
                for (DatastoreNode node : nodes.values()) {
                    Hibernate.initialize(node.getChildNodeFullPaths());
                }
                return nodes;
            });
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

    private DatastoreNode testNode(Map<String, DatastoreNode> datastoreNodes, String name,
        boolean regexp, int expectedChildNodeCount, DatastoreNode parentNode) {
        String parentFullPath = parentNode != null ? parentNode.getFullPath() : "";
        String fullPath = DatastoreConfigurationImporter.fullPathFromParentPath(name,
            parentFullPath);
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
        importerSpy.importConfiguration();

        dataFileImporter = new DatastoreConfigurationImporter(List.of(UPDATE_FILE), false);
        DatastoreConfigurationImporter updaterSpy = Mockito.spy(dataFileImporter);
        setMocks(updaterSpy);
        updaterSpy.importConfiguration();

        @SuppressWarnings("unchecked")
        Map<String, DatastoreRegexp> regexpsByName = (Map<String, DatastoreRegexp>) DatabaseTransactionFactory
            .performTransaction(() -> regexpCrud.retrieveRegexpsByName());
        @SuppressWarnings("unchecked")
        Map<String, DatastoreNode> nodesByFullPath = (Map<String, DatastoreNode>) DatabaseTransactionFactory
            .performTransaction(() -> {
                Map<String, DatastoreNode> nodes = nodeCrud.retrieveNodesByFullPath();
                for (DatastoreNode node : nodes.values()) {
                    Hibernate.initialize(node.getChildNodeFullPaths());
                }
                return nodes;
            });

        assertNotNull(regexpsByName.get("sector"));
        assertEquals("(sector-[0-9]{4})", regexpsByName.get("sector").getValue());
        assertNotNull(regexpsByName.get("cadenceType"));
        assertEquals("(target|ffi|fast-target)", regexpsByName.get("cadenceType").getValue());
        assertEquals(2, regexpsByName.size());

        DatastoreNode sectorNode = testNode(nodesByFullPath, "sector", true, 1, null);
        DatastoreNode mdaNode = testNode(nodesByFullPath, "mda", false, 2, sectorNode);

        // DR nodes.
        DatastoreNode drNode = testNode(nodesByFullPath, "dr", false, 1, mdaNode);
        DatastoreNode drPixelNode = testNode(nodesByFullPath, "pixels", false, 1, drNode);
        DatastoreNode drCadenceTypeNode = testNode(nodesByFullPath, "cadenceType", true, 1,
            drPixelNode);
        testNode(nodesByFullPath, "ccd", false, 0, drCadenceTypeNode);

        // PA nodes.
        DatastoreNode paNode = testNode(nodesByFullPath, "pa", false, 1, mdaNode);
        DatastoreNode paFluxNode = testNode(nodesByFullPath, "raw-flux", false, 1, paNode);
        DatastoreNode paCadenceTypeNode = testNode(nodesByFullPath, "cadenceType", true, 1,
            paFluxNode);
        testNode(nodesByFullPath, "ccd", false, 0, paCadenceTypeNode);

        // Deleted nodes.
        assertNull(nodesByFullPath.get("sector/mda/dr/pixels/cadenceType/channel"));
        assertNull(nodesByFullPath.get("sector/mda/cal"));
        assertNull(nodesByFullPath.get("sector/mda/cal/pixels"));
        assertNull(nodesByFullPath.get("sector/mda/cal/pixels/cadenceType"));
        assertNull(nodesByFullPath.get("sector/mda/cal/pixels/cadenceType/channel"));

        assertEquals(10, nodesByFullPath.size());
    }

    // Dry run test -- should import but not persist
    @Test
    public void testDryRun() throws JAXBException {

        DatastoreConfigurationImporter dataFileImporter = new DatastoreConfigurationImporter(
            ImmutableList.of(FILE_1, FILE_2), true);
        DatastoreConfigurationImporter importerSpy = Mockito.spy(dataFileImporter);
        setMocks(importerSpy);
        importerSpy.importConfiguration();

        assertEquals(3, importerSpy.getDataFileTypes().size());
        Mockito.verify(dataFileTypeCrud, Mockito.times(0))
            .persist(ArgumentMatchers.<DataFileType> anyList());
        assertEquals(2, importerSpy.getModelTypes().size());
        Mockito.verify(modelCrud, Mockito.times(0)).persist(ArgumentMatchers.<ModelType> anyList());
    }

    @Test
    public void testDryRunOfUpdate() {
        DatastoreConfigurationImporter dataFileImporter = new DatastoreConfigurationImporter(
            ImmutableList.of(FILE_1, FILE_2), false);
        DatastoreConfigurationImporter importerSpy = Mockito.spy(dataFileImporter);
        setMocks(importerSpy);
        importerSpy.importConfiguration();

        dataFileImporter = new DatastoreConfigurationImporter(List.of(UPDATE_FILE), true);
        DatastoreConfigurationImporter updaterSpy = Mockito.spy(dataFileImporter);
        setMocks(updaterSpy);
        updaterSpy.importConfiguration();

        @SuppressWarnings("unchecked")
        Map<String, DatastoreRegexp> regexpsByName = (Map<String, DatastoreRegexp>) DatabaseTransactionFactory
            .performTransaction(() -> regexpCrud.retrieveRegexpsByName());
        @SuppressWarnings("unchecked")
        Map<String, DatastoreNode> nodesByFullPath = (Map<String, DatastoreNode>) DatabaseTransactionFactory
            .performTransaction(() -> {
                Map<String, DatastoreNode> nodes = new DatastoreNodeCrud()
                    .retrieveNodesByFullPath();
                for (DatastoreNode node : nodes.values()) {
                    Hibernate.initialize(node.getChildNodeFullPaths());
                }
                return nodes;
            });

        assertEquals(2, regexpsByName.size());
        DatastoreRegexp regexp = regexpsByName.get("cadenceType");
        assertNotNull(regexp);
        assertEquals("(target|ffi)", regexp.getValue());
        regexp = regexpsByName.get("sector");
        assertNotNull(regexp);
        assertEquals("(sector-[0-9]{4})", regexp.getValue());

        DatastoreNode sectorNode = testNode(nodesByFullPath, "sector", true, 1, null);
        DatastoreNode mdaNode = testNode(nodesByFullPath, "mda", false, 2, sectorNode);

        // DR nodes
        DatastoreNode drNode = testNode(nodesByFullPath, "dr", false, 1, mdaNode);
        DatastoreNode drPixelNode = testNode(nodesByFullPath, "pixels", false, 1, drNode);
        DatastoreNode drCadenceTypeNode = testNode(nodesByFullPath, "cadenceType", true, 1,
            drPixelNode);
        testNode(nodesByFullPath, "channel", false, 0, drCadenceTypeNode);

        // CAL nodes
        DatastoreNode calNode = testNode(nodesByFullPath, "cal", false, 1, mdaNode);
        DatastoreNode calPixelNode = testNode(nodesByFullPath, "pixels", false, 1, calNode);
        DatastoreNode calCadenceTypeNode = testNode(nodesByFullPath, "cadenceType", true, 1,
            calPixelNode);
        testNode(nodesByFullPath, "channel", false, 0, calCadenceTypeNode);

        assertEquals(10, nodesByFullPath.size());
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
        Mockito.verify(dataFileTypeCrud, Mockito.times(1))
            .persist(ArgumentMatchers.<DataFileType> anyList());
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
        Mockito.verify(dataFileTypeCrud, Mockito.times(1))
            .persist(ArgumentMatchers.<DataFileType> anyList());
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
        Mockito.verify(dataFileTypeCrud, Mockito.times(1))
            .persist(ArgumentMatchers.<DataFileType> anyList());
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
        Mockito.when(dataFileImporter.dataFileTypeCrud()).thenReturn(dataFileTypeCrud);
        Mockito.when(dataFileImporter.modelCrud()).thenReturn(modelCrud);
        Mockito.when(dataFileImporter.datastoreRegexpCrud()).thenReturn(regexpCrud);
        Mockito.when(dataFileImporter.datastoreNodeCrud()).thenReturn(nodeCrud);
    }
}
