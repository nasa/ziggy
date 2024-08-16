package gov.nasa.ziggy.uow;

import static gov.nasa.ziggy.services.config.PropertyName.DATASTORE_ROOT_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.data.datastore.DatastoreRegexp;
import gov.nasa.ziggy.data.datastore.DatastoreTestUtils;
import gov.nasa.ziggy.data.datastore.DatastoreWalker;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionNodeOperations;
import gov.nasa.ziggy.services.config.DirectoryProperties;

/**
 * Unit test class for DatastoreDirectoryUnitOfWorkGenerator.
 *
 * @author PT
 */
public class DatastoreDirectoryUnitOfWorkTest {

    private PipelineInstanceNode pipelineInstanceNode;
    private PipelineDefinitionNode pipelineDefinitionNode;
    private DatastoreDirectoryUnitOfWorkGenerator uowGenerator;
    private DataFileType drSciencePixels;
    private PipelineDefinitionNodeOperations pipelineDefinitionNodeOperations;

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    public ZiggyPropertyRule datastoreRootDirPropertyRule = new ZiggyPropertyRule(
        DATASTORE_ROOT_DIR, directoryRule);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(directoryRule)
        .around(datastoreRootDirPropertyRule);

    @Before
    public void setup() throws IOException {

        // Create the datastore.
        DatastoreTestUtils.createDatastoreDirectories();

        // Create data file types.
        drSciencePixels = new DataFileType("dr science pixels",
            "sector/mda/dr/pixels/cadenceType/pixelType$science/channel", "dummy1");

        // Create the pipeline instance node and pipeline definition node.
        pipelineInstanceNode = Mockito.mock(PipelineInstanceNode.class);
        pipelineDefinitionNode = Mockito.mock(PipelineDefinitionNode.class);
        Mockito.when(pipelineDefinitionNode.getInputDataFileTypes())
            .thenReturn(Set.of(drSciencePixels));
        Mockito.when(pipelineInstanceNode.getPipelineDefinitionNode())
            .thenReturn(pipelineDefinitionNode);
        pipelineDefinitionNodeOperations = Mockito.mock(PipelineDefinitionNodeOperations.class);
        Mockito.when(pipelineDefinitionNodeOperations.inputDataFileTypes(pipelineDefinitionNode))
            .thenReturn(Set.of(drSciencePixels));

        // Create the datastore walker and the UOW generator.
        DatastoreWalker datastoreWalker = new DatastoreWalker(DatastoreTestUtils.regexpsByName(),
            DatastoreTestUtils.datastoreNodesByFullPath());
        uowGenerator = Mockito.spy(DatastoreDirectoryUnitOfWorkGenerator.class);
        Mockito.doReturn(datastoreWalker).when(uowGenerator).datastoreWalker();
        Mockito.doReturn(pipelineDefinitionNodeOperations)
            .when(uowGenerator)
            .pipelineDefinitionNodeOperations();
    }

    /**
     * Basic functionality test -- makes sure that the expected behavior in terms of included and
     * excluded directories, brief states, etc., is obtained.
     */
    @Test
    public void testGenerateUnitsOfWork() {

        List<UnitOfWork> uowList = PipelineExecutor.generateUnitsOfWork(uowGenerator,
            pipelineInstanceNode);

        // construct a map of expected results
        Map<String, String> uowMap = new HashMap<>();
        for (UnitOfWork uow : uowList) {
            uowMap.put(DirectoryUnitOfWorkGenerator.directory(uow), uow.briefState());
        }

        // Check the contents of the Map
        String path = DirectoryProperties.datastoreRootDir()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:A")
            .toAbsolutePath()
            .toString();
        assertTrue(uowMap.containsKey(path));
        assertEquals(uowMap.get(path), "[sector-0002;target;1:1:A]");
        path = DirectoryProperties.datastoreRootDir()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:B")
            .toAbsolutePath()
            .toString();
        assertTrue(uowMap.containsKey(path));
        assertEquals(uowMap.get(path), "[sector-0002;target;1:1:B]");

        path = DirectoryProperties.datastoreRootDir()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("science")
            .resolve("1:1:A")
            .toAbsolutePath()
            .toString();
        assertTrue(uowMap.containsKey(path));
        assertEquals(uowMap.get(path), "[sector-0002;ffi;1:1:A]");
        path = DirectoryProperties.datastoreRootDir()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("science")
            .resolve("1:1:B")
            .toAbsolutePath()
            .toString();
        assertTrue(uowMap.containsKey(path));
        assertEquals(uowMap.get(path), "[sector-0002;ffi;1:1:B]");

        path = DirectoryProperties.datastoreRootDir()
            .resolve("sector-0003")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:A")
            .toAbsolutePath()
            .toString();
        assertTrue(uowMap.containsKey(path));
        assertEquals(uowMap.get(path), "[sector-0003;target;1:1:A]");
        path = DirectoryProperties.datastoreRootDir()
            .resolve("sector-0003")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:B")
            .toAbsolutePath()
            .toString();
        assertTrue(uowMap.containsKey(path));
        assertEquals(uowMap.get(path), "[sector-0003;target;1:1:B]");

        path = DirectoryProperties.datastoreRootDir()
            .resolve("sector-0003")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("science")
            .resolve("1:1:A")
            .toAbsolutePath()
            .toString();
        assertTrue(uowMap.containsKey(path));
        assertEquals(uowMap.get(path), "[sector-0003;ffi;1:1:A]");
        path = DirectoryProperties.datastoreRootDir()
            .resolve("sector-0003")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("science")
            .resolve("1:1:B")
            .toAbsolutePath()
            .toString();
        assertTrue(uowMap.containsKey(path));
        assertEquals(uowMap.get(path), "[sector-0003;ffi;1:1:B]");

        assertEquals(8, uowMap.size());
    }

    // Test that include and exclude restrictions on the DatastoreRegexps are correctly
    // handled.
    @Test
    public void testGenerateUnitsOfWorkWithIncludesAndExcludes() {
        Map<String, DatastoreRegexp> regexpsByName = DatastoreTestUtils.regexpsByName();
        DatastoreRegexp regexp = regexpsByName.get("sector");
        regexp.setInclude("sector-0002");
        regexp = regexpsByName.get("cadenceType");
        regexp.setExclude("ffi");

        Mockito
            .doReturn(
                new DatastoreWalker(regexpsByName, DatastoreTestUtils.datastoreNodesByFullPath()))
            .when(uowGenerator)
            .datastoreWalker();

        List<UnitOfWork> uowList = PipelineExecutor.generateUnitsOfWork(uowGenerator,
            pipelineInstanceNode);

        // construct a map of expected results
        Map<String, String> briefStateByDirectory = new HashMap<>();
        Map<String, UnitOfWork> uowByBriefState = new HashMap<>();
        for (UnitOfWork uow : uowList) {
            briefStateByDirectory.put(DirectoryUnitOfWorkGenerator.directory(uow),
                uow.briefState());
            uowByBriefState.put(uow.briefState(), uow);
        }
        // Check the contents of the Map
        String path = DirectoryProperties.datastoreRootDir()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:A")
            .toAbsolutePath()
            .toString();
        assertTrue(briefStateByDirectory.containsKey(path));
        assertEquals(briefStateByDirectory.get(path), "[1:1:A]");
        path = DirectoryProperties.datastoreRootDir()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:B")
            .toAbsolutePath()
            .toString();
        assertTrue(briefStateByDirectory.containsKey(path));
        assertEquals(briefStateByDirectory.get(path), "[1:1:B]");

        // Test the capture of regexp values.
        UnitOfWork uow = uowByBriefState.get("[1:1:A]");
        assertNotNull(uow.getParameter("sector"));
        assertEquals("sector-0002", uow.getParameter("sector").getString());
        assertNotNull(uow.getParameter("cadenceType"));
        assertEquals("target", uow.getParameter("cadenceType").getString());
        assertNotNull(uow.getParameter("pixelType"));
        assertEquals("science", uow.getParameter("pixelType").getString());
        assertNotNull(uow.getParameter("channel"));
        assertEquals("1:1:A", uow.getParameter("channel").getString());

        // Test the capture of regexp values.
        uow = uowByBriefState.get("[1:1:B]");
        assertNotNull(uow.getParameter("sector"));
        assertEquals("sector-0002", uow.getParameter("sector").getString());
        assertNotNull(uow.getParameter("cadenceType"));
        assertEquals("target", uow.getParameter("cadenceType").getString());
        assertNotNull(uow.getParameter("pixelType"));
        assertEquals("science", uow.getParameter("pixelType").getString());
        assertNotNull(uow.getParameter("channel"));
        assertEquals("1:1:B", uow.getParameter("channel").getString());
    }

    // Tests UOW generation with multiple directories per UOW.
    @Test
    public void testUowMultipleDirectories() {

        // Create two data file types: target and collateral.
        DataFileType targetSciencePixels = new DataFileType("target science pixels",
            "sector/mda/dr/pixels/cadenceType$target/pixelType$science/channel", "dummy2");
        DataFileType collateralSciencePixels = new DataFileType("collateral science pixels",
            "sector/mda/dr/pixels/cadenceType$target/pixelType$collateral/channel", "dummy3");

        Mockito.when(pipelineDefinitionNodeOperations.inputDataFileTypes(pipelineDefinitionNode))
            .thenReturn(Set.of(targetSciencePixels, collateralSciencePixels));

        List<UnitOfWork> uowList = PipelineExecutor.generateUnitsOfWork(uowGenerator,
            pipelineInstanceNode);
        Map<String, UnitOfWork> uowsByName = new HashMap<>();
        for (UnitOfWork uow : uowList) {
            uowsByName.put(uow.briefState(), uow);
        }

        testUow(uowsByName.get("[sector-0002;1:1:A]"), "sector-0002", "1:1:A");
        testUow(uowsByName.get("[sector-0002;1:1:B]"), "sector-0002", "1:1:B");
        testUow(uowsByName.get("[sector-0003;1:1:A]"), "sector-0003", "1:1:A");
        testUow(uowsByName.get("[sector-0003;1:1:B]"), "sector-0003", "1:1:B");
        assertEquals(4, uowList.size());
    }

    /** Performs all necessary tests on a {@link UnitOfWork} instance. */
    private void testUow(UnitOfWork uow, String sector, String channel) {
        assertNotNull(uow);

        // Test that the correct directories are present.
        List<String> directories = DirectoryUnitOfWorkGenerator.directories(uow);
        assertTrue(directories.contains(DirectoryProperties.datastoreRootDir()
            .resolve(sector)
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve(channel)
            .toAbsolutePath()
            .toString()));
        assertTrue(directories.contains(DirectoryProperties.datastoreRootDir()
            .resolve(sector)
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("collateral")
            .resolve(channel)
            .toAbsolutePath()
            .toString()));
        assertNotNull(DirectoryUnitOfWorkGenerator.directory(uow));
        assertEquals(2, directories.size());

        // Test that the mapping from data file type to directory is correct.
        Map<String, String> directoriesByDataFileType = DirectoryUnitOfWorkGenerator
            .directoriesByDataFileType(uow);
        assertEquals(2, directoriesByDataFileType.size());
        String dataFileTypeDirectory = directoriesByDataFileType.get("target science pixels");
        assertNotNull(dataFileTypeDirectory);
        assertEquals(DirectoryProperties.datastoreRootDir()
            .resolve(sector)
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve(channel)
            .toAbsolutePath()
            .toString(), dataFileTypeDirectory);
        dataFileTypeDirectory = directoriesByDataFileType.get("collateral science pixels");
        assertNotNull(dataFileTypeDirectory);
        assertEquals(DirectoryProperties.datastoreRootDir()
            .resolve(sector)
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("collateral")
            .resolve(channel)
            .toAbsolutePath()
            .toString(), dataFileTypeDirectory);
    }

    @Test
    public void testUowMultipleDirectoriesWithIncludes() {

        // Create two data file types: target and collateral.
        DataFileType targetSciencePixels = new DataFileType("target science pixels",
            "sector/mda/dr/pixels/cadenceType$target/pixelType$science/channel", "dummy4");
        DataFileType collateralSciencePixels = new DataFileType("collateral science pixels",
            "sector/mda/dr/pixels/cadenceType$target/pixelType$collateral/channel", "dummy5");

        Mockito.when(pipelineDefinitionNodeOperations.inputDataFileTypes(pipelineDefinitionNode))
            .thenReturn(Set.of(targetSciencePixels, collateralSciencePixels));

        // Create an include restriction.
        Map<String, DatastoreRegexp> regexpsByName = DatastoreTestUtils.regexpsByName();
        DatastoreRegexp regexp = regexpsByName.get("sector");
        regexp.setInclude("sector-0002");

        Mockito
            .doReturn(
                new DatastoreWalker(regexpsByName, DatastoreTestUtils.datastoreNodesByFullPath()))
            .when(uowGenerator)
            .datastoreWalker();

        List<UnitOfWork> uowList = PipelineExecutor.generateUnitsOfWork(uowGenerator,
            pipelineInstanceNode);
        Map<String, UnitOfWork> uowsByName = new HashMap<>();
        for (UnitOfWork uow : uowList) {
            uowsByName.put(uow.briefState(), uow);
        }

        testUow(uowsByName.get("[1:1:A]"), "sector-0002", "1:1:A");
        testUow(uowsByName.get("[1:1:B]"), "sector-0002", "1:1:B");
        assertEquals(2, uowList.size());

        // The pixel type regexp value should be missing, since we are using both science and
        // collateral pixels in this UOW.
        UnitOfWork uow = uowsByName.get("[1:1:A]");
        assertNotNull(uow.getParameter("sector"));
        assertEquals("sector-0002", uow.getParameter("sector").getString());
        assertNotNull(uow.getParameter("cadenceType"));
        assertEquals("target", uow.getParameter("cadenceType").getString());
        assertNull(uow.getParameter("pixelType"));
        assertNotNull(uow.getParameter("channel"));
        assertEquals("1:1:A", uow.getParameter("channel").getString());
        assertNull(uow.getParameter("pixelType"));

        uow = uowsByName.get("[1:1:B]");
        assertNotNull(uow.getParameter("sector"));
        assertEquals("sector-0002", uow.getParameter("sector").getString());
        assertNotNull(uow.getParameter("cadenceType"));
        assertEquals("target", uow.getParameter("cadenceType").getString());
        assertNull(uow.getParameter("pixelType"));
        assertNotNull(uow.getParameter("channel"));
        assertEquals("1:1:B", uow.getParameter("channel").getString());
        assertNull(uow.getParameter("pixelType"));
    }

    @Test
    public void testGenerateUowsSingleUowSingleDataFileType() {
        Map<String, DatastoreRegexp> regexpsByName = DatastoreTestUtils.regexpsByName();
        DatastoreRegexp regexp = regexpsByName.get("sector");
        regexp.setInclude("sector-0002");
        regexp = regexpsByName.get("cadenceType");
        regexp.setExclude("ffi");
        regexp = regexpsByName.get("channel");
        regexp.setInclude("1:1:A");

        Mockito
            .doReturn(
                new DatastoreWalker(regexpsByName, DatastoreTestUtils.datastoreNodesByFullPath()))
            .when(uowGenerator)
            .datastoreWalker();

        List<UnitOfWork> uowList = PipelineExecutor.generateUnitsOfWork(uowGenerator,
            pipelineInstanceNode);
        assertEquals(1, uowList.size());
        UnitOfWork uow = uowList.get(0);
        assertEquals("[sector-0002;target;1:1:A]", uow.briefState());
        assertEquals("sector-0002", uow.getParameter("sector").getString());
        assertEquals("target", uow.getParameter("cadenceType").getString());
        assertEquals("1:1:A", uow.getParameter("channel").getString());
        assertEquals("science", uow.getParameter("pixelType").getString());
    }

    @Test
    public void testGenerateUowsSingleUowMultipleDataFileTypes() {
        Map<String, DatastoreRegexp> regexpsByName = DatastoreTestUtils.regexpsByName();
        DatastoreRegexp regexp = regexpsByName.get("sector");
        regexp.setInclude("sector-0002");
        regexp = regexpsByName.get("cadenceType");
        regexp.setExclude("ffi");
        regexp = regexpsByName.get("channel");
        regexp.setInclude("1:1:A");

        Mockito
            .doReturn(
                new DatastoreWalker(regexpsByName, DatastoreTestUtils.datastoreNodesByFullPath()))
            .when(uowGenerator)
            .datastoreWalker();

        // Create another file type.
        DataFileType drCollateralPixels = new DataFileType("dr science pixels",
            "sector/mda/dr/pixels/cadenceType/pixelType$collateral/channel", "dummy6");

        Mockito.when(pipelineDefinitionNodeOperations.inputDataFileTypes(pipelineDefinitionNode))
            .thenReturn(Set.of(drSciencePixels, drCollateralPixels));

        List<UnitOfWork> uowList = PipelineExecutor.generateUnitsOfWork(uowGenerator,
            pipelineInstanceNode);
        assertEquals(1, uowList.size());
        UnitOfWork uow = uowList.get(0);
        assertEquals("[sector-0002;target;1:1:A]", uow.briefState());
        assertEquals("sector-0002", uow.getParameter("sector").getString());
        assertEquals("target", uow.getParameter("cadenceType").getString());
        assertEquals("1:1:A", uow.getParameter("channel").getString());
        assertNull(uow.getParameter("pixelType"));
    }
}
