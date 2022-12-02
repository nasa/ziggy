package gov.nasa.ziggy.module.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

import org.junit.Test;

/**
 * @author Todd Klaus
 */
public class TimestampFileTest {
    @Test
    public void test() throws IOException {
        File expectedFile = null;

        try {
            long timeMillis = System.currentTimeMillis();
            String expectedName = "QUEUED_PBS." + timeMillis;

            File directory = new File("/tmp");

            boolean success = TimestampFile.create(directory, TimestampFile.Event.QUEUED_PBS,
                timeMillis);

            assertTrue("success", success);

            expectedFile = new File("/tmp", expectedName);

            assertTrue("expected file", expectedFile.exists());

            Path path = expectedFile.toPath();
            Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(path);
            assertTrue("owner can reaad", permissions.contains(PosixFilePermission.OWNER_READ));
            assertTrue("group can reaad", permissions.contains(PosixFilePermission.GROUP_READ));
            assertTrue("other can reaad", permissions.contains(PosixFilePermission.OTHERS_READ));

            long actualTime = TimestampFile.timestamp(directory, TimestampFile.Event.QUEUED_PBS);

            assertEquals("timestamp", timeMillis, actualTime);
        } finally {
            expectedFile.delete();
        }
    }
}
