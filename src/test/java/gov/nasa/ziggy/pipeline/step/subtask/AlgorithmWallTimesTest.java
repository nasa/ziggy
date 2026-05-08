package gov.nasa.ziggy.pipeline.step.subtask;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.step.TimestampFile;
import gov.nasa.ziggy.pipeline.step.TimestampFile.Event;
import gov.nasa.ziggy.pipeline.step.subtask.AlgorithmWallTimes.SubtaskWallTime;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;

public class AlgorithmWallTimesTest {

    public ZiggyDirectoryRule ziggyDirectoryRule = new ZiggyDirectoryRule();

    public ZiggyPropertyRule resultsDirRule = new ZiggyPropertyRule(
        PropertyName.RESULTS_DIR.property(), ziggyDirectoryRule);

    @Rule
    public RuleChain ziggyRuleChain = RuleChain.outerRule(ziggyDirectoryRule)
        .around(resultsDirRule);

    private PipelineTask pipelineTask = Mockito.mock(PipelineTask.class);
    private String taskBaseName = "100-200-whatever";

    @Before
    public void setUp() throws IOException {

        Mockito.when(pipelineTask.taskBaseName()).thenReturn(taskBaseName);
        Files.createDirectories(
            DirectoryProperties.taskDataDir().resolve(taskBaseName).resolve("st-0"));
        Files.createDirectories(
            DirectoryProperties.taskDataDir().resolve(taskBaseName).resolve("st-1"));
        Files.createDirectories(
            DirectoryProperties.taskDataDir().resolve(taskBaseName).resolve("st-2"));
        Files.createDirectories(
            DirectoryProperties.taskDataDir().resolve(taskBaseName).resolve("st-3"));

        TimestampFile.create(
            DirectoryProperties.taskDataDir().resolve(taskBaseName).resolve("st-0").toFile(),
            Event.SUBTASK_START);
        TimestampFile.create(
            DirectoryProperties.taskDataDir().resolve(taskBaseName).resolve("st-0").toFile(),
            Event.SUBTASK_FINISH);

        TimestampFile.create(
            DirectoryProperties.taskDataDir().resolve(taskBaseName).resolve("st-1").toFile(),
            Event.SUBTASK_FINISH);

        TimestampFile.create(
            DirectoryProperties.taskDataDir().resolve(taskBaseName).resolve("st-2").toFile(),
            Event.SUBTASK_START);

        TimestampFile.create(
            DirectoryProperties.taskDataDir().resolve(taskBaseName).resolve("st-3").toFile(),
            Event.SUBTASK_START);
        TimestampFile.create(
            DirectoryProperties.taskDataDir().resolve(taskBaseName).resolve("st-3").toFile(),
            Event.SUBTASK_FINISH);
    }

    @Test
    public void testWriteAndReadHdf5() {

        AlgorithmWallTimes.generateSubtaskWallTimesFile(pipelineTask);
        assertTrue(Files.isRegularFile(ziggyDirectoryRule.directory()
            .resolve("run")
            .resolve(taskBaseName)
            .resolve(AlgorithmWallTimes.FILE_NAME)));
        AlgorithmWallTimes wallTimesFromFile = AlgorithmWallTimes
            .readSubtaskWallTimesFile(pipelineTask);
        List<AlgorithmWallTimes.SubtaskWallTime> wallTimes = wallTimesFromFile.subtaskWallTimes();
        assertEquals(2, wallTimes.size());
        List<Integer> subtaskIndices = wallTimes.stream()
            .map(SubtaskWallTime::getSubtaskIndex)
            .toList();
        assertTrue(subtaskIndices.contains(0));
        assertTrue(subtaskIndices.contains(3));
    }
}
