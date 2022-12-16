package gov.nasa.ziggy.util.io;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.TestEventDetector;
import gov.nasa.ziggy.ZiggyDirectoryRule;

/**
 * Unit tests for {@link LockManager}. Note that these tests are limited to a single process.
 *
 * @author Mark Rose
 * @author PT
 */
public class LockManagerTest {

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    private Path lockFileOne;
    private Path lockFileTwo;

    @Before
    public void setUp() throws IOException {
        lockFileOne = directoryRule.directory().resolve("one.lock");
        lockFileTwo = directoryRule.directory().resolve("two.lock");
        createLockFile(lockFileOne);
        createLockFile(lockFileTwo);
    }

    @After
    public void tearDown() throws IOException {
        LockManager.releaseAllLocks();
    }

    private void createLockFile(Path p) throws IOException {
        File f = p.toFile();
        f.delete();
        f.getParentFile().mkdirs();
        FileUtils.touch(f);
    }

    /**
     * Tests that a read lock does not block other readers.
     *
     * @throws InterruptedException
     */
    @Test
    public void testReadLockDoesNotBlockReader() throws IOException, InterruptedException {
        LockManager.getReadLock(lockFileOne.toFile());
        ReaderTask task = new ReaderTask(lockFileOne);
        task.start();
        task.join();
        assertTrue(task.isLockObtained());
        assertFalse(task.isBlocked());
        LockManager.releaseReadLock(lockFileOne.toFile());
    }

    /**
     * Tests that using the write lock with blocking does block readers.
     */
    @Test
    public void testWriteLockBlocksReader() throws IOException, InterruptedException {
        LockManager.getWriteLockOrBlock(lockFileOne.toFile());
        ReaderTask task = new ReaderTask(lockFileOne);
        task.start();
        TestEventDetector.detectTestEvent(500L, () -> {
            return task.isLockAttempted();
        });
        assertTrue(task.isBlocked());
        assertFalse(task.isLockObtained());
        LockManager.releaseWriteLock(lockFileOne.toFile());
        task.join();
        assertFalse(task.isBlocked());
        assertTrue(task.isLockObtained());
    }

    /**
     * Tests that the write locker that doesn't block its callers still does, nonetheless, block
     * readers.
     */
    @Test
    public void tesNonBlockingtWriteLockBlocksReader() throws IOException, InterruptedException {
        LockManager.getWriteLockWithoutBlocking(lockFileOne.toFile());
        ReaderTask task = new ReaderTask(lockFileOne);
        task.start();
        TestEventDetector.detectTestEvent(500L, () -> {
            return task.isLockAttempted();
        });
        assertTrue(task.isBlocked());
        assertFalse(task.isLockObtained());
        LockManager.releaseWriteLock(lockFileOne.toFile());
        task.join();
        assertFalse(task.isBlocked());
        assertTrue(task.isLockObtained());
    }

    /**
     * Tests that using the getWriteLockOrBlock method does, in fact, block until such time as the
     * write lock becomes available.
     */
    @Test
    public void testWriteLockBlocksWriter() throws IOException, InterruptedException {
        LockManager.getWriteLockWithoutBlocking(lockFileOne.toFile());
        BlockingWriterTask task = new BlockingWriterTask(lockFileOne);
        task.start();
        TestEventDetector.detectTestEvent(500L, () -> {
            return task.isLockAttempted();
        });
        assertTrue(task.isBlocked());
        assertFalse(task.isLockObtained());
        LockManager.releaseWriteLock(lockFileOne.toFile());
        task.join();
        assertFalse(task.isBlocked());
        assertTrue(task.isLockObtained());
    }

    /**
     * Tests that using the getWriteLockWithoutBlocking method doesn't block but also doesn't get a
     * lock that's already held.
     */
    @Test
    public void testWriteLockDoesNotBlockWriter() throws IOException, InterruptedException {
        LockManager.getWriteLockWithoutBlocking(lockFileOne.toFile());
        NonBlockingWriterTask task = new NonBlockingWriterTask(lockFileOne);
        task.start();
        TestEventDetector.detectTestEvent(500L, () -> {
            return task.isDoneTryingLock();
        });
        assertFalse(task.isBlocked());
        assertFalse(task.isLockObtained());
        LockManager.releaseWriteLock(lockFileOne.toFile());
        task.join();
        assertFalse(task.isBlocked());
        assertFalse(task.isLockObtained());
    }

    /**
     * Tests that the getWriteLockWithoutBlocking method returns true when the lock is obtained.
     */
    @Test
    public void testNonBlockingWriteLockReturnValue() throws InterruptedException {

        NonBlockingWriterTask task = new NonBlockingWriterTask(lockFileOne);
        task.start();
        task.join();
        assertTrue(task.isLockObtained());

    }

    /**
     * Abstract parent class to all the classes that attempt to get various kinds of locks. The
     * {@link AbstractTask} class provides a {@link #run()} method that attempts to obtain a lock
     * and then releases it, meanwhile providing information to the user as to the current state of
     * the attempt (attempt started, blocked, lock obtained, etc.). This allows the caller to test
     * the state of the {@link AbstractTask} instance to make sure it's as expected.
     *
     * @author Mark Rose
     * @author PT
     */
    private static abstract class AbstractTask extends Thread {

        private Path lockFile;
        private boolean blocked = false;
        private boolean lockObtained = false;
        private boolean lockAttempted = false;
        private boolean doneTryingLock = false;

        public AbstractTask(Path lockFile) {
            this.lockFile = lockFile;
        }

        protected abstract void getLock() throws IOException;

        protected abstract void releaseLock() throws IOException;

        @Override
        public void run() {
            try {
                blocked = true;
                lockAttempted = true;
                getLock();
                lockObtained = true;
                blocked = false;
                doneTryingLock = true;
                releaseLock();
            } catch (IOException ex) {
                System.err.println("Error waiting for read lock: " + ex);
                ex.printStackTrace();
            }
        }

        protected Path getLockFile() {
            return lockFile;
        }

        public boolean isBlocked() {
            return blocked;
        }

        public boolean isLockObtained() {
            return lockObtained;
        }

        public boolean isLockAttempted() {
            return lockAttempted;
        }

        public boolean isDoneTryingLock() {
            return doneTryingLock;
        }

    }

    /**
     * Attempts to obtain a read lock.
     *
     * @author Mark Rose
     * @author PT
     */
    private static class ReaderTask extends AbstractTask {

        public ReaderTask(Path lockFile) {
            super(lockFile);
        }

        @Override
        protected void getLock() throws IOException {
            LockManager.getReadLock(getLockFile().toFile());
        }

        @Override
        protected void releaseLock() throws IOException {
            LockManager.releaseReadLock(getLockFile().toFile());
        }

    }

    /**
     * Attempts to obtain a write lock but does not get blocked if it fails. This class has to have
     * its own boolean that indicates whether the lock was obtained, and it overrides the
     * {@link #isLockObtained()} method to use the subclass boolean rather than the abstract class
     * boolean.
     *
     * @author Mark Rose
     * @author PT
     */
    private static class NonBlockingWriterTask extends AbstractTask {

        boolean obtainedLock;

        public NonBlockingWriterTask(Path lockFile) {
            super(lockFile);
        }

        @Override
        protected void getLock() throws IOException {
            obtainedLock = LockManager.getWriteLockWithoutBlocking(getLockFile().toFile());
        }

        @Override
        public boolean isLockObtained() {
            return obtainedLock;
        }

        @Override
        protected void releaseLock() throws IOException {
            if (obtainedLock) {
                LockManager.releaseWriteLock(getLockFile().toFile());
            }
        }

    }

    /**
     * Attempts to obtain a write lock, and blocks until it obtains it.
     *
     * @author Mark Rose
     * @author PT
     */
    private static class BlockingWriterTask extends AbstractTask {

        public BlockingWriterTask(Path lockFile) {
            super(lockFile);
        }

        @Override
        protected void getLock() throws IOException {
            LockManager.getWriteLockOrBlock(getLockFile().toFile());
        }

        @Override
        protected void releaseLock() throws IOException {
            LockManager.releaseWriteLock(getLockFile().toFile());
        }

    }

}
