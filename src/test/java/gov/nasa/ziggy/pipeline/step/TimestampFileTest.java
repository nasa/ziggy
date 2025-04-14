package gov.nasa.ziggy.pipeline.step;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
import gov.nasa.ziggy.pipeline.step.TimestampFile.Event;

/**
 * @author Todd Klaus
 */
public class TimestampFileTest {

    private PipelineTask pipelineTask = Mockito.mock(PipelineTask.class);
    private PipelineTaskDataOperations pipelineTaskDataOperations = Mockito
        .mock(PipelineTaskDataOperations.class);

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Before
    public void SetUp() {
        TimestampFile.setPipelineTaskDataOperations(pipelineTaskDataOperations);
    }

    @Test
    public void testCreate() throws IOException {
        long timeMillis = System.currentTimeMillis();
        String expectedName = "QUEUED." + timeMillis;

        File directory = directoryRule.directory().toFile();

        boolean success = TimestampFile.create(directory, TimestampFile.Event.QUEUED, timeMillis);

        assertTrue("success", success);

        File expectedFile = directoryRule.directory().resolve(expectedName).toFile();

        assertTrue("expected file", expectedFile.exists());

        Path path = expectedFile.toPath();
        Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(path);
        assertTrue("owner can reaad", permissions.contains(PosixFilePermission.OWNER_READ));
        assertTrue("group can reaad", permissions.contains(PosixFilePermission.GROUP_READ));
        assertTrue("other can reaad", permissions.contains(PosixFilePermission.OTHERS_READ));

        long actualTime = TimestampFile.timestamp(directory, TimestampFile.Event.QUEUED);

        assertEquals("timestamp", timeMillis, actualTime);
    }

    @Test
    public void testArriveComputeNodes() throws IOException {
        testEventWithEarlyDefault(Event.ARRIVE_COMPUTE_NODES);
    }

    @Test
    public void testQueued() throws IOException {
        testEventWithEarlyDefault(Event.QUEUED);
    }

    @Test
    public void testStart() throws IOException {
        testEventWithEarlyDefault(Event.START);
    }

    @Test
    public void testSubtaskStart() throws IOException {
        testEventWithEarlyDefault(Event.SUBTASK_START);
    }

    private void testEventWithEarlyDefault(TimestampFile.Event event) throws IOException {
        Mockito
            .when(
                pipelineTaskDataOperations.startTimestamp(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(1_000_000L);

        // No timestamp files.
        long eventTime = TimestampFile.eventTimeMillis(directoryRule.directory().toFile(), event,
            pipelineTask);
        assertEquals(1000_000L, eventTime);
        assertFalse(TimestampFile.exists(directoryRule.directory().toFile(), event));

        // One timestamp file.
        TimestampFile.create(directoryRule.directory().toFile(), event, 1_000_001L);
        eventTime = TimestampFile.eventTimeMillis(directoryRule.directory().toFile(), event,
            pipelineTask);
        assertEquals(1000_001L, eventTime);

        // Two timestamp files.
        Files.createFile(directoryRule.directory().resolve(event.toString() + ".1000002"));
        eventTime = TimestampFile.eventTimeMillis(directoryRule.directory().toFile(), event,
            pipelineTask);
        assertEquals(1000_001L, eventTime);
    }

    @Test
    public void testFinish() throws IOException {
        testEventWithLateDefault(Event.FINISH);
    }

    @Test
    public void testSubtaskFinish() throws IOException {
        testEventWithLateDefault(Event.SUBTASK_FINISH);
    }

    private void testEventWithLateDefault(TimestampFile.Event event) throws IOException {
        Mockito
            .when(pipelineTaskDataOperations.endTimestamp(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(1_000_000L);

        // No timestamp files.
        long eventTime = TimestampFile.eventTimeMillis(directoryRule.directory().toFile(), event,
            pipelineTask);
        assertEquals(1000_000L, eventTime);
        assertFalse(TimestampFile.exists(directoryRule.directory().toFile(), event));

        // One timestamp file.
        TimestampFile.create(directoryRule.directory().toFile(), event, 1_000_001L);
        eventTime = TimestampFile.eventTimeMillis(directoryRule.directory().toFile(), event,
            pipelineTask);
        assertEquals(1000_001L, eventTime);

        // Two timestamp files.
        Files.createFile(directoryRule.directory().resolve(event.toString() + ".1000002"));
        eventTime = TimestampFile.eventTimeMillis(directoryRule.directory().toFile(), event,
            pipelineTask);
        assertEquals(1000_002L, eventTime);
    }

    @Test
    public void testElapsedTimeMillis() throws IOException {
        Mockito
            .when(
                pipelineTaskDataOperations.startTimestamp(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(1_000_000L);
        Mockito
            .when(pipelineTaskDataOperations.endTimestamp(ArgumentMatchers.any(PipelineTask.class)))
            .thenReturn(1_000_005L);

        // No timestamp files.
        assertEquals(5L, TimestampFile.elapsedTimeMillis(directoryRule.directory().toFile(),
            Event.START, Event.FINISH, pipelineTask));

        // One early file, no late ones.
        TimestampFile.create(directoryRule.directory().toFile(), Event.START, 1_000_001L);
        assertEquals(4L, TimestampFile.elapsedTimeMillis(directoryRule.directory().toFile(),
            Event.START, Event.FINISH, pipelineTask));

        // One late file, one early one.
        TimestampFile.create(directoryRule.directory().toFile(), Event.FINISH, 1_000_004L);
        assertEquals(3L, TimestampFile.elapsedTimeMillis(directoryRule.directory().toFile(),
            Event.START, Event.FINISH, pipelineTask));

        // One late file, no early one.
        TimestampFile.delete(directoryRule.directory().toFile(), Event.START);
        assertEquals(4L, TimestampFile.elapsedTimeMillis(directoryRule.directory().toFile(),
            Event.START, Event.FINISH, pipelineTask));

        // Multiple early, one late.
        TimestampFile.create(directoryRule.directory().toFile(), Event.START, 1_000_001L);
        Files.createFile(directoryRule.directory().resolve(Event.START.toString() + ".1000002"));
        assertEquals(3L, TimestampFile.elapsedTimeMillis(directoryRule.directory().toFile(),
            Event.START, Event.FINISH, pipelineTask));

        // Multiple early, multiple late.
        Files.createFile(directoryRule.directory().resolve(Event.FINISH.toString() + ".1000006"));
        assertEquals(5L, TimestampFile.elapsedTimeMillis(directoryRule.directory().toFile(),
            Event.START, Event.FINISH, pipelineTask));
    }
}
