package gov.nasa.ziggy.data.datastore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;

/**
 * Unit tests for the {@link DatastoreWalker} class.
 *
 * @author PT
 */
public class DatastoreWalkerTest {

    private DatastoreWalker datastoreWalker;

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();
    public ZiggyPropertyRule datastoreRootPropertyRule = new ZiggyPropertyRule(
        PropertyName.DATASTORE_ROOT_DIR.property(), directoryRule, "datastore");

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule(directoryRule)
        .around(datastoreRootPropertyRule);

    @Before
    public void setUp() throws IOException {
        datastoreWalker = new DatastoreWalker(DatastoreTestUtils.regexpsByName(),
            DatastoreTestUtils.datastoreNodesByFullPath());
        DatastoreTestUtils.createDatastoreDirectories();
    }

    @Test
    public void testLocationExists() {
        assertTrue(datastoreWalker.locationExists("sector/mda/dr/pixels/cadenceType/pixelType"));
        assertTrue(datastoreWalker.locationExists("sector/mda/dr/pixels/cadenceType$ffi"));
        assertFalse(datastoreWalker.locationExists("sector/foo/dr"));
        assertFalse(datastoreWalker.locationExists("sector/mda/cal/pixels/cadenceType$foo"));
        assertFalse(datastoreWalker.locationExists("sector/mda/dr/pixels/cadenceType$ffi$target"));
    }

    @Test
    public void testPathsForLocation() throws IOException {
        Path datastoreRoot = DirectoryProperties.datastoreRootDir();

        List<Path> paths = datastoreWalker
            .pathsForLocation("sector/mda/dr/pixels/cadenceType$ffi/pixelType$science/channel");
        assertEquals(4, paths.size());

        assertTrue(paths.contains(datastoreRoot.toAbsolutePath()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("science")
            .resolve("1:1:A")));
        assertTrue(paths.contains(datastoreRoot.toAbsolutePath()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("science")
            .resolve("1:1:B")));
        assertTrue(paths.contains(datastoreRoot.toAbsolutePath()
            .resolve("sector-0003")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("science")
            .resolve("1:1:A")));
        assertTrue(paths.contains(datastoreRoot.toAbsolutePath()
            .resolve("sector-0003")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("science")
            .resolve("1:1:B")));

        paths = datastoreWalker
            .pathsForLocation("sector/mda/dr/pixels/cadenceType/pixelType/channel");
        assertEquals(16, paths.size());

        assertTrue(paths.contains(datastoreRoot.toAbsolutePath()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("science")
            .resolve("1:1:A")));
        assertTrue(paths.contains(datastoreRoot.toAbsolutePath()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("science")
            .resolve("1:1:B")));
        assertTrue(paths.contains(datastoreRoot.toAbsolutePath()
            .resolve("sector-0003")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("science")
            .resolve("1:1:A")));
        assertTrue(paths.contains(datastoreRoot.toAbsolutePath()
            .resolve("sector-0003")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("science")
            .resolve("1:1:B")));

        assertTrue(paths.contains(datastoreRoot.toAbsolutePath()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("collateral")
            .resolve("1:1:A")));
        assertTrue(paths.contains(datastoreRoot.toAbsolutePath()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("collateral")
            .resolve("1:1:B")));
        assertTrue(paths.contains(datastoreRoot.toAbsolutePath()
            .resolve("sector-0003")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("collateral")
            .resolve("1:1:A")));
        assertTrue(paths.contains(datastoreRoot.toAbsolutePath()
            .resolve("sector-0003")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("collateral")
            .resolve("1:1:B")));

        assertTrue(paths.contains(datastoreRoot.toAbsolutePath()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:A")));
        assertTrue(paths.contains(datastoreRoot.toAbsolutePath()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:B")));
        assertTrue(paths.contains(datastoreRoot.toAbsolutePath()
            .resolve("sector-0003")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:A")));
        assertTrue(paths.contains(datastoreRoot.toAbsolutePath()
            .resolve("sector-0003")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:B")));

        assertTrue(paths.contains(datastoreRoot.toAbsolutePath()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("collateral")
            .resolve("1:1:A")));
        assertTrue(paths.contains(datastoreRoot.toAbsolutePath()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("collateral")
            .resolve("1:1:B")));
        assertTrue(paths.contains(datastoreRoot.toAbsolutePath()
            .resolve("sector-0003")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("collateral")
            .resolve("1:1:A")));
        assertTrue(paths.contains(datastoreRoot.toAbsolutePath()
            .resolve("sector-0003")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("collateral")
            .resolve("1:1:B")));

        // Now test with include and exclude regular expressions.
        Map<String, DatastoreRegexp> regexpsByName = datastoreWalker.regexpsByName();
        regexpsByName.get("sector").setInclude("sector-0002");
        regexpsByName.get("channel").setExclude("1:1:A");

        paths = datastoreWalker
            .pathsForLocation("sector/mda/dr/pixels/cadenceType$ffi/pixelType$science/channel");
        assertEquals(1, paths.size());
        assertTrue(paths.contains(datastoreRoot.toAbsolutePath()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("science")
            .resolve("1:1:B")));
    }

    @Test
    public void testDatastoreDirectoryBriefState() throws IOException {
        int datastoreRootPathElements = DirectoryProperties.datastoreRootDir()
            .toAbsolutePath()
            .getNameCount();

        List<Path> paths = datastoreWalker
            .pathsForLocation("sector/mda/dr/pixels/cadenceType$ffi/pixelType$science/channel");

        List<Integer> pathElementIndices = datastoreWalker.pathElementIndicesForBriefState(paths);
        assertTrue(pathElementIndices.contains(datastoreRootPathElements + 0));
        assertTrue(pathElementIndices.contains(datastoreRootPathElements + 6));
        assertEquals(2, pathElementIndices.size());

        // Now test with include and exclude regular expressions.
        Map<String, DatastoreRegexp> regexpsByName = datastoreWalker.regexpsByName();
        regexpsByName.get("sector").setInclude("sector-0002");
        regexpsByName.get("channel").setExclude("1:1:A");

        paths = datastoreWalker
            .pathsForLocation("sector/mda/dr/pixels/cadenceType$ffi/pixelType$science/channel");

        pathElementIndices = datastoreWalker.pathElementIndicesForBriefState(paths);
        assertTrue(pathElementIndices.isEmpty());
    }

    @Test
    public void testLocationMatchesDatastore() {
        assertTrue(datastoreWalker.locationMatchesDatastore("sector-0002/mda"));
        assertTrue(datastoreWalker.locationMatchesDatastore("sector-0003/mda/dr/pixels"));
        assertFalse(datastoreWalker.locationMatchesDatastore("sector/mda"));
        assertFalse(datastoreWalker.locationMatchesDatastore("sector-0003/tba"));
        assertFalse(
            datastoreWalker.locationMatchesDatastore("sector-0003/mda/dr/pixels/cadenceType$ffi"));
        assertFalse(datastoreWalker
            .locationMatchesDatastore("sector-0003/mda/dr/pixels/ffi/collateral/1:1:A/subdir"));
    }

    @Test
    public void testRegexpValues() {
        Path datastoreRoot = DirectoryProperties.datastoreRootDir();
        Map<String, String> regexpValues = datastoreWalker.regexpValues(
            "sector/mda/dr/pixels/cadenceType$ffi/pixelType$science/channel",
            datastoreRoot.toAbsolutePath()
                .resolve("sector-0002")
                .resolve("mda")
                .resolve("dr")
                .resolve("pixels")
                .resolve("ffi")
                .resolve("science")
                .resolve("1:1:B"));
        assertNotNull(regexpValues.get("sector"));
        assertEquals("sector-0002", regexpValues.get("sector"));
        assertNotNull(regexpValues.get("cadenceType"));
        assertEquals("ffi", regexpValues.get("cadenceType"));
        assertNotNull(regexpValues.get("pixelType"));
        assertEquals("science", regexpValues.get("pixelType"));
        assertNotNull(regexpValues.get("channel"));
        assertEquals("1:1:B", regexpValues.get("channel"));
        assertEquals(4, regexpValues.size());
    }

    @Test
    public void testRegexpValuesWithLocationSuppression() {
        Path datastoreRoot = DirectoryProperties.datastoreRootDir();
        Map<String, String> regexpValues = datastoreWalker.regexpValuesByRegexpName(
            "sector/mda/dr/pixels/cadenceType$ffi/pixelType$science/channel",
            datastoreRoot.toAbsolutePath()
                .resolve("sector-0002")
                .resolve("mda")
                .resolve("dr")
                .resolve("pixels")
                .resolve("ffi")
                .resolve("science")
                .resolve("1:1:B"),
            false);
        assertNotNull(regexpValues.get("sector"));
        assertEquals("sector-0002", regexpValues.get("sector"));
        assertNotNull(regexpValues.get("channel"));
        assertEquals("1:1:B", regexpValues.get("channel"));
        assertEquals(2, regexpValues.size());
    }

    @Test
    public void testPathFromLocationAndRegexpValues() {
        Path datastoreRoot = DirectoryProperties.datastoreRootDir();
        Map<String, String> regexpValues = datastoreWalker.regexpValues(
            "sector/mda/dr/pixels/cadenceType$ffi/pixelType$science/channel",
            datastoreRoot.toAbsolutePath()
                .resolve("sector-0002")
                .resolve("mda")
                .resolve("dr")
                .resolve("pixels")
                .resolve("ffi")
                .resolve("science")
                .resolve("1:1:B"));
        Path constructedPath = datastoreWalker.pathFromLocationAndRegexpValues(regexpValues,
            "sector/mda/dr/pixels/cadenceType$target/pixelType$collateral/channel");
        assertEquals(datastoreRoot.toAbsolutePath()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("collateral")
            .resolve("1:1:B")
            .toString(), constructedPath.toString());
    }

    /**
     * Tests whether the pathFromLocationAndRegexpValues method does the right thing when one of the
     * regexps is missing from the Map of values but has a value assigned in the location argument.
     */
    @Test
    public void testPathFromRegexValuesWhenPartNotInRegexpMap() {
        Path datastoreRoot = DirectoryProperties.datastoreRootDir();
        Map<String, String> regexpValues = new HashMap<>();
        regexpValues.put("sector", "sector-0002");
        regexpValues.put("cadenceType", "target");
        regexpValues.put("channel", "1:1:B");
        Path constructedPath = datastoreWalker.pathFromLocationAndRegexpValues(regexpValues,
            "sector/mda/dr/pixels/cadenceType$target/pixelType$collateral/channel");
        assertEquals(datastoreRoot.toAbsolutePath()
            .resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("collateral")
            .resolve("1:1:B")
            .toString(), constructedPath.toString());
    }

    @Test
    public void testSpecificLocation() {

        // First case: the fileNameRegexp has no location parts in it.
        Map<String, DataFileType> dataFileTypesByName = DatastoreTestUtils.dataFileTypesByName();
        DataFileType dataFileType = dataFileTypesByName.get("uncalibrated science pixel values");

        // Simple example: the path is a subset of the location.
        String specificLocation = datastoreWalker.specificLocation(dataFileType,
            DirectoryProperties.datastoreRootDir()
                .resolve(Paths.get("sector/mda/dr/pixels/cadenceType")));
        assertEquals("sector/mda/dr/pixels/cadenceType/pixelType$science/channel",
            specificLocation);

        // The path has a regexp value in it.
        specificLocation = datastoreWalker.specificLocation(dataFileType,
            DirectoryProperties.datastoreRootDir()
                .resolve(Paths.get("sector/mda/dr/pixels/target")));
        assertEquals("sector/mda/dr/pixels/cadenceType$target/pixelType$science/channel",
            specificLocation);

        // The path has a regexp value in it that matches a regexp constraint in the location.
        specificLocation = datastoreWalker.specificLocation(dataFileType,
            DirectoryProperties.datastoreRootDir()
                .resolve(Paths.get("sector/mda/dr/pixels/cadenceType/science")));
        assertEquals("sector/mda/dr/pixels/cadenceType/pixelType$science/channel",
            specificLocation);

        // Second case: the fileNameRegexp has location parts in it.
        dataFileTypesByName = DatastoreTestUtils.dataFileTypesByNameRegexpsInFileName();
        dataFileType = dataFileTypesByName.get("uncalibrated science pixel values");

        // Simple example: the path is a subset of the location.
        specificLocation = datastoreWalker.specificLocation(dataFileType,
            DirectoryProperties.datastoreRootDir()
                .resolve(Paths.get("sector/mda/dr/pixels/cadenceType")));
        assertEquals("sector/mda/dr/pixels/cadenceType/pixelType$science/channel",
            specificLocation);

        // The path has a regexp value in it.
        specificLocation = datastoreWalker.specificLocation(dataFileType,
            DirectoryProperties.datastoreRootDir()
                .resolve(Paths.get("sector/mda/dr/pixels/target")));
        assertEquals("sector/mda/dr/pixels/cadenceType$target/pixelType$science/channel",
            specificLocation);

        // The path has a regexp value in it that matches a regexp constraint in the location.
        specificLocation = datastoreWalker.specificLocation(dataFileType,
            DirectoryProperties.datastoreRootDir()
                .resolve(Paths.get("sector/mda/dr/pixels/cadenceType/science")));
        assertEquals("sector/mda/dr/pixels/cadenceType/pixelType$science/channel",
            specificLocation);
    }

    @Test(expected = PipelineException.class)
    public void testExceptionOnInvalidSpecificLocation() {
        Map<String, DataFileType> dataFileTypesByName = DatastoreTestUtils.dataFileTypesByName();
        DataFileType dataFileType = dataFileTypesByName.get("uncalibrated science pixel values");
        datastoreWalker.specificLocation(dataFileType,
            DirectoryProperties.datastoreRootDir()
                .toAbsolutePath()
                .resolve(Paths.get("sector/mda/dr/pixels/cadenceType/collateral/1:1:A")));
    }

    @Test
    public void testPathsForLocationForMissionLocation() {
        Path directoryToRemove = DirectoryProperties.datastoreRootDir()
            .toAbsolutePath()
            .resolve(Paths.get("sector-0003/mda/cal"));
        ZiggyFileUtils.deleteDirectoryTree(directoryToRemove, true);
        List<Path> paths = datastoreWalker
            .pathsForLocation("sector/mda/cal/pixels/cadenceType/pixelType/channel");

        // All we care about is that the test didn't error out when it tried to get
        // directories under sector-0003/mda/cal.
        assertFalse(CollectionUtils.isEmpty(paths));
    }

    @Test
    public void testFindLocationIndicesForSublocation() {

        // First case: the fileNameRegexp has no location parts in it.
        Map<String, DataFileType> dataFileTypesByName = DatastoreTestUtils.dataFileTypesByName();
        DataFileType dataFileType = dataFileTypesByName.get("uncalibrated science pixel values");
        assertTrue(datastoreWalker.findLocationIndicesForSublocation(dataFileType).isEmpty());

        // Second case: the fileNameRegexp has location parts in it.
        dataFileTypesByName = DatastoreTestUtils.dataFileTypesByNameRegexpsInFileName();
        dataFileType = dataFileTypesByName.get("uncalibrated science pixel values");
        assertEquals(Set.of(1), datastoreWalker.findLocationIndicesForSublocation(dataFileType));
    }

    @Test
    public void testGetDataFileTypeConvenienceFields() {

        // First case: the fileNameRegexp has no location parts in it.
        Map<String, DataFileType> dataFileTypesByName = DatastoreTestUtils.dataFileTypesByName();
        DataFileType dataFileType = dataFileTypesByName.get("uncalibrated science pixel values");
        assertEquals(dataFileType.getFileNameRegexp(),
            DatastoreWalker.fileNameRegexpBaseName(dataFileType));
        assertEquals(dataFileType.getLocation(), DatastoreWalker.fullLocation(dataFileType));

        // Second case: the fileNameRegexp has location parts in it.
        dataFileTypesByName = DatastoreTestUtils.dataFileTypesByNameRegexpsInFileName();
        dataFileType = dataFileTypesByName.get("uncalibrated science pixel values");
        assertEquals(dataFileType.getLocation() + "/pixelType$science/channel",
            DatastoreWalker.fullLocation(dataFileType));
        assertEquals("(uncalibrated-pixels-[0-9]+)\\.science\\.nc",
            DatastoreWalker.fileNameRegexpBaseName(dataFileType));
    }
}
