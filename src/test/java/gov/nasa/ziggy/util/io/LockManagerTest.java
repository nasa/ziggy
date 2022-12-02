package gov.nasa.ziggy.util.io;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link LockManager}. Note that these tests are limited to a single process.
 */
public class LockManagerTest {

    private static final File LOCK_FILE_ONE = new File("build/test/LockManagerTest/one.lock");
    private static final File LOCK_FILE_TWO = new File("build/test/LockManagerTest/two.lock");

    @Before
    public void setup() throws IOException {
        createLockFile(LOCK_FILE_ONE);
        createLockFile(LOCK_FILE_TWO);
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deleteDirectory(new File(Filenames.BUILD_TEST));
    }

    private void createLockFile(File f) throws IOException {
        f.delete();
        f.getParentFile().mkdirs();
        FileUtils.touch(f);
    }

    /**
     * Tests that a read lock does not block other readers.
     */
    @Test
    public void testReadLockDoesNotBlockReader() throws IOException {
        LockManager.getReadLock(LOCK_FILE_ONE);
        ReaderTask task = new ReaderTask(LOCK_FILE_ONE);
        task.start();

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            // Ignore
        }

        LockManager.releaseReadLock(LOCK_FILE_ONE);

        assertTrue(task.getDelay() < 100);
    }

    /**
     * Tests that using the write lock with blocking does block readers.
     */
    @Test
    public void testWriteLockBlocksReader() throws IOException, InterruptedException {
        LockManager.getWriteLockOrBlock(LOCK_FILE_ONE);
        ReaderTask task = new ReaderTask(LOCK_FILE_ONE);
        task.start();

        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            // Ignore
        }

        LockManager.releaseWriteLock(LOCK_FILE_ONE);

        task.join();
        assertTrue(task.getDelay() > 100);
    }

    /**
     * Tests that the write locker that doesn't block its callers still does, nonetheless, block
     * readers.
     */
    @Test
    public void tesNonBlockingtWriteLockBlocksReader() throws IOException, InterruptedException {
        LockManager.getWriteLockWithoutBlocking(LOCK_FILE_ONE);
        ReaderTask task = new ReaderTask(LOCK_FILE_ONE);
        task.start();

        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            // Ignore
        }

        LockManager.releaseWriteLock(LOCK_FILE_ONE);

        task.join();
        assertTrue(task.getDelay() > 100);
    }

    /**
     * Tests that using the getWriteLockOrBlock method does, in fact, block until such time as the
     * write lock becomes available.
     */
    @Test
    public void testWriteLockBlocksWriter() throws IOException, InterruptedException {
        LockManager.getWriteLockWithoutBlocking(LOCK_FILE_ONE);
        BlockingWriterTask task = new BlockingWriterTask(LOCK_FILE_ONE);
        task.start();

        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            // Ignore
        }

        LockManager.releaseWriteLock(LOCK_FILE_ONE);

        task.join();
        assertTrue(task.getDelay() > 100);
    }

    /**
     * Tests that using the getWriteLockWithoutBlocking method doesn't block but also doesn't get a
     * lock that's already held.
     */
    @Test
    public void testWriteLockDoesNotBlockWriter() throws IOException, InterruptedException {
        LockManager.getWriteLockWithoutBlocking(LOCK_FILE_ONE);
        NonBlockingWriterTask task = new NonBlockingWriterTask(LOCK_FILE_ONE);
        task.start();

        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            // Ignore
        }

        LockManager.releaseWriteLock(LOCK_FILE_ONE);

        task.join();
        assertTrue(task.getDelay() < 100);
        assertFalse(task.obtainedLock);
    }

    /**
     * Tests that the getWriteLockWithoutBlocking method returns true when the lock is obtained.
     */
    @Test
    public void testNonBlockingWriteLockReturnValue() throws InterruptedException {

        NonBlockingWriterTask task = new NonBlockingWriterTask(LOCK_FILE_ONE);
        task.start();
        task.join();
        assertTrue(task.obtainedLock);

    }

    private static abstract class AbstractTask extends Thread {

        private File lockFile;
        private long lockDelay = 0;

        public AbstractTask(File lockFile) {
            this.lockFile = lockFile;
        }

        protected abstract void getLock() throws IOException;

        protected abstract void releaseLock() throws IOException;

        @Override
        public void run() {
            try {
                long start = System.currentTimeMillis();
                getLock();
                lockDelay = System.currentTimeMillis() - start;
                releaseLock();
            } catch (IOException ex) {
                System.err.println("Error waiting for read lock: " + ex);
                ex.printStackTrace();
                lockDelay = -1;
            }
        }

        public long getDelay() {
            return lockDelay;
        }

        protected File getLockFile() {
            return lockFile;
        }

    }

    private static class ReaderTask extends AbstractTask {

        public ReaderTask(File lockFile) {
            super(lockFile);
        }

        @Override
        protected void getLock() throws IOException {
            LockManager.getReadLock(getLockFile());
        }

        @Override
        protected void releaseLock() throws IOException {
            LockManager.releaseReadLock(getLockFile());
        }

    }

    private static class NonBlockingWriterTask extends AbstractTask {

        boolean obtainedLock;

        public NonBlockingWriterTask(File lockFile) {
            super(lockFile);
        }

        @Override
        protected void getLock() throws IOException {
            obtainedLock = LockManager.getWriteLockWithoutBlocking(getLockFile());
        }

        @Override
        protected void releaseLock() throws IOException {
            if (obtainedLock) {
                LockManager.releaseWriteLock(getLockFile());
            }
        }

    }

    private static class BlockingWriterTask extends AbstractTask {

        public BlockingWriterTask(File lockFile) {
            super(lockFile);
        }

        @Override
        protected void getLock() throws IOException {
            LockManager.getWriteLockOrBlock(getLockFile());
        }

        @Override
        protected void releaseLock() throws IOException {
            LockManager.releaseWriteLock(getLockFile());
        }

    }

}
