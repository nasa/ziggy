package gov.nasa.ziggy.module;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.data.datastore.DatastoreFileManager;
import gov.nasa.ziggy.data.datastore.DatastoreTestUtils;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;

/**
 * Unit tests for {@link DatastoreDirectoryPipelineOutputs} class.
 * <p>
 * Note that the method {@link DatastoreDirectoryPipelineOutputs#copyTaskFilesToDatastore()} is not
 * tested here. That method does nothing but call a method in {@link DatastoreFileManager}, so the
 * unit tests of the latter class should be sufficient to guarantee that the method in the former
 * class will work as expected.
 *
 * @author PT
 */
public class DatastoreDirectoryPipelineOutputsTest {

    private static final int EXPECTED_SUBTASK_COUNT = 7;

    public ZiggyDirectoryRule ziggyDirectoryRule = new ZiggyDirectoryRule();

    public ZiggyPropertyRule datastoreRootProperty = new ZiggyPropertyRule(
        PropertyName.DATASTORE_ROOT_DIR, ziggyDirectoryRule, "datastore");

    public ZiggyPropertyRule taskDirRule = new ZiggyPropertyRule(PropertyName.RESULTS_DIR,
        ziggyDirectoryRule, "pipeline-results");

    @Rule
    public final RuleChain testRuleChain = RuleChain.outerRule(ziggyDirectoryRule)
        .around(datastoreRootProperty)
        .around(taskDirRule);

    private Path taskDirectory;
    private DataFileType calibratedSciencePixelsDataFileType;
    private DataFileType calibratedCollateralPixelsDataFileType;

    @Before
    public void setup() throws IOException {

        taskDirectory = DirectoryProperties.taskDataDir();

        // Get and update the data file types.
        Map<String, DataFileType> dataFileTypes = DatastoreTestUtils.dataFileTypesByName();
        calibratedSciencePixelsDataFileType = dataFileTypes.get("calibrated science pixel values");
        calibratedSciencePixelsDataFileType
            .setFileNameRegexp("calibrated-pixels-[0-9]+\\.science\\.nc");
        calibratedCollateralPixelsDataFileType = dataFileTypes
            .get("calibrated collateral pixel values");
        calibratedCollateralPixelsDataFileType
            .setFileNameRegexp("calibrated-pixels-[0-9]+\\.collateral\\.nc");

        // Construct the subtask directories and the outputs files.
        constructOutputsFiles("calibrated-pixels-", ".science.nc", EXPECTED_SUBTASK_COUNT);
        constructOutputsFiles("calibrated-pixels-", ".collateral.nc", EXPECTED_SUBTASK_COUNT - 1);

        // Construct a directory with no outputs files.
        SubtaskUtils.createSubtaskDirectory(taskDirectory, EXPECTED_SUBTASK_COUNT + 1);

        // Construct the collection of output file types in the task directory.
        Set<DataFileType> outputDataFileTypes = Set.of(calibratedSciencePixelsDataFileType,
            calibratedCollateralPixelsDataFileType);
        PipelineInputsOutputsUtils.serializeOutputFileTypesToTaskDirectory(outputDataFileTypes,
            taskDirectory);
    }

    private Set<Path> constructOutputsFiles(String fileNamePrefix, String fileNameSuffix,
        int subtaskDirCount) throws IOException {
        Set<Path> paths = new HashSet<>();
        for (int subtaskIndex = 0; subtaskIndex < subtaskDirCount; subtaskIndex++) {
            Path subtaskDir = SubtaskUtils.createSubtaskDirectory(taskDirectory, subtaskIndex);
            paths.add(Files
                .createFile(subtaskDir.resolve(fileNamePrefix + subtaskIndex + fileNameSuffix)));
        }
        return paths;
    }

    @Test
    public void testSubtaskProducedOutputs() {

        DatastoreDirectoryPipelineOutputs pipelineOutputs = new DatastoreDirectoryPipelineOutputs();
        for (int subtaskIndex = 0; subtaskIndex < EXPECTED_SUBTASK_COUNT; subtaskIndex++) {
            assertTrue(pipelineOutputs.subtaskProducedOutputs(taskDirectory,
                SubtaskUtils.subtaskDirectory(taskDirectory, subtaskIndex)));
        }
        assertFalse(pipelineOutputs.subtaskProducedOutputs(taskDirectory,
            SubtaskUtils.subtaskDirectory(taskDirectory, EXPECTED_SUBTASK_COUNT + 1)));
    }
}
