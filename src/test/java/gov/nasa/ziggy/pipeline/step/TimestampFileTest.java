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

import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.pipeline.step.TimestampFile.Event;
import gov.nasa.ziggy.util.SystemProxy;

/**
 * @author Todd Klaus
 */
public class TimestampFileTest {

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

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

        long actualTime = TimestampFile.eventTimeMillis(directory, TimestampFile.Event.QUEUED);

        assertEquals("timestamp", timeMillis, actualTime);
    }

    @Test
    public void testCreateIfAbsent() {
        long timeMillis = System.currentTimeMillis();
        SystemProxy.setUserTime(timeMillis);
        String expectedName = "QUEUED." + timeMillis;
        File directory = directoryRule.directory().toFile();
        boolean success = TimestampFile.create(directory, TimestampFile.Event.QUEUED);
        assertTrue("success", success);
        File expectedFile = directoryRule.directory().resolve(expectedName).toFile();
        assertTrue("expected file", expectedFile.exists());
        long newTimeMillis = timeMillis + 1;
        SystemProxy.setUserTime(newTimeMillis);
        success = TimestampFile.createIfAbsent(directory, TimestampFile.Event.QUEUED);
        assertTrue("success", success);
        expectedFile = directoryRule.directory().resolve(expectedName).toFile();
        assertTrue("expected file", expectedFile.exists());
        File unexpectedFile = directoryRule.directory().resolve("QUEUED." + newTimeMillis).toFile();
        assertFalse("unexpected file", unexpectedFile.exists());
    }

    @Test
    public void testDeleteAllTaskLevelTimestamps() {
        for (Event event : TimestampFile.Event.values()) {
            TimestampFile.create(directoryRule.directory().toFile(), event);
        }
        boolean allDeleted = TimestampFile
            .deleteAllTaskLevelTimestamps(directoryRule.directory().toFile());
        assertTrue(allDeleted);
        for (Event event : TimestampFile.Event.values()) {
            if (event.isTaskLevelTimestamp()) {
                assertFalse(TimestampFile.exists(directoryRule.directory().toFile(), event));
            } else {
                assertTrue(TimestampFile.exists(directoryRule.directory().toFile(), event));
            }
        }
    }

    @Test
    public void testArriveComputeNodes() throws IOException {
        testEvent(Event.ARRIVE_COMPUTE_NODES);
    }

    @Test
    public void testQueued() throws IOException {
        testEvent(Event.QUEUED);
    }

    @Test
    public void testStart() throws IOException {
        testEvent(Event.START);
    }

    @Test
    public void testSubtaskStart() throws IOException {
        testEvent(Event.SUBTASK_START);
    }

    private void testEvent(TimestampFile.Event event) throws IOException {
        // No timestamp files.
        long eventTime = TimestampFile.eventTimeMillis(directoryRule.directory().toFile(), event);
        assertEquals(-1L, eventTime);
        assertFalse(TimestampFile.exists(directoryRule.directory().toFile(), event));

        // One timestamp file.
        TimestampFile.create(directoryRule.directory().toFile(), event, 1_000_001L);
        eventTime = TimestampFile.eventTimeMillis(directoryRule.directory().toFile(), event);
        assertEquals(1000_001L, eventTime);
    }

    @Test
    public void testFinish() throws IOException {
        testEvent(Event.FINISH);
    }

    @Test
    public void testSubtaskFinish() throws IOException {
        testEvent(Event.SUBTASK_FINISH);
    }

    @Test
    public void testElapsedTimeMillis() throws IOException {

        // No timestamp files.
        assertEquals(-1L, TimestampFile.elapsedTimeMillis(directoryRule.directory().toFile(),
            Event.START, Event.FINISH));

        // Start file but no end file.
        TimestampFile.create(directoryRule.directory().toFile(), Event.START, 1_000_001L);
        SystemProxy.setUserTime(1_000_004L);
        assertEquals(3L, TimestampFile.elapsedTimeMillis(directoryRule.directory().toFile(),
            Event.START, Event.FINISH));

        // Both start and end files
        TimestampFile.create(directoryRule.directory().toFile(), Event.FINISH, 1_000_008L);
        assertEquals(7L, TimestampFile.elapsedTimeMillis(directoryRule.directory().toFile(),
            Event.START, Event.FINISH));
    }

    @Test
    public void testElapsedTimeValid() {
        assertFalse(TimestampFile.elapsedTimeValid(directoryRule.directory().toFile(),
            Event.SUBTASK_START, Event.SUBTASK_FINISH));
        TimestampFile.create(directoryRule.directory().toFile(), Event.SUBTASK_START, 1_000_001L);
        assertFalse(TimestampFile.elapsedTimeValid(directoryRule.directory().toFile(),
            Event.SUBTASK_START, Event.SUBTASK_FINISH));
        TimestampFile.create(directoryRule.directory().toFile(), Event.SUBTASK_FINISH, 1_000_002L);
        assertTrue(TimestampFile.elapsedTimeValid(directoryRule.directory().toFile(),
            Event.SUBTASK_START, Event.SUBTASK_FINISH));
    }
}
