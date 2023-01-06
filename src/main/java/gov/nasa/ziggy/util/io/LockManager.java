package gov.nasa.ziggy.util.io;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import gov.nasa.ziggy.util.ZiggyShutdownHook;

/**
 * Implements locking and unlocking on files in a way that works both among Java threads in a single
 * process and among multiple Java processes.
 */
public enum LockManager {

    /** The singleton instance. */
    INSTANCE();

    LockManager() {
        ZiggyShutdownHook.addShutdownHook(() -> {
            try {
                releaseAllLocks();
            } catch (IOException e) {
                // We can swallow this exception because it occurs during a shutdown. Therefore,
                // by definition there's nothing to be done because we're just trying to exit
                // Ziggy...
            }
        });
    }

    /**
     * Releases all locks held by the {@link LockManager} singleton instance. For use in testing and
     * in the shutdown hook.
     *
     * @throws IOException if exception occurs when releasing locks.
     */
    public static synchronized void releaseAllLocks() throws IOException {
        for (File f : INSTANCE.readerChannels.keySet()) {
            releaseReadLock(f);
        }
        for (File f : INSTANCE.writerChannels.keySet()) {
            releaseWriteLock(f);
        }
    }

    private Map<File, ReentrantReadWriteLock> locks = new HashMap<>();
    private Map<File, AtomicInteger> readerCounts = new HashMap<>();

    private Map<File, FileChannel> readerChannels = new HashMap<>();
    private Map<File, FileChannel> writerChannels = new HashMap<>();

    private synchronized ReentrantReadWriteLock getLock(File f) {
        ReentrantReadWriteLock lock = locks.get(f);
        if (lock == null) {
            lock = new ReentrantReadWriteLock();
            locks.put(f, lock);
        }

        return lock;
    }

    private synchronized AtomicInteger getReaderCount(File f) {
        AtomicInteger readerCount = readerCounts.get(f);
        if (readerCount == null) {
            readerCount = new AtomicInteger();
            readerCounts.put(f, readerCount);
        }

        return readerCount;
    }

    /**
     * Gets a read lock on a file. A read lock precludes other threads or processes getting a write
     * lock, but does not prevent other readers. If the caller is the first thread to request a read
     * lock on the file, an OS file lock is obtained.
     *
     * @param f the file on which to get the lock
     * @throws IOException if there is an error accessing the file
     */
    public static void getReadLock(File f) throws IOException {
        INSTANCE.getReadLockInternal(f);
    }

    /**
     * Releases a read lock on a file. If this is the last read lock, releases the OS lock on the
     * file.
     *
     * @param f the file on which to lock
     * @throws IOException if there is an error accessing the file
     */
    public static void releaseReadLock(File f) throws IOException {
        INSTANCE.releaseReadLockInternal(f);
    }

    /**
     * Gets a write lock on a file. A write lock precludes other threads or processes getting a read
     * or write lock. This is ensured by first obtaining a process-wide Java write lock and then
     * obtaining an OS write lock.
     * <p>
     * Callers that use this method will block if they are unable to obtain the write lock. For
     * obtaining a write lock that doesn't block if the attempt is not successful, see
     * {@link #getWriteLockWithoutBlocking(File)}.
     *
     * @param f the file on which to get the lock
     * @throws IOException if there is an error accessing the file
     */
    public static void getWriteLockOrBlock(File f) throws IOException {
        INSTANCE.getWriteLockOrBlockInternal(f);
    }

    /**
     * Gets a write lock on a file. A write lock precludes other threads or processes getting a read
     * or write lock. This is ensured by first obtaining a process-wide Java write lock and then
     * obtaining an OS write lock.
     * <p>
     * Callers that use this method will not block if they are unable to obtain the write lock. If
     * it is important for callers to block if they do not obtain the write lock, use
     * {@link #getWriteLockOrBlock(File)}.
     *
     * @param f the file on which to get the lock.
     * @return true if the lock is obtained, false otherwise. A return of false indicates that
     * either another thread in the current process holds the lock, or that another process entirely
     * holds it.
     * @throws IOException if there is an error accessing the file.
     */
    public static boolean getWriteLockWithoutBlocking(File f) throws IOException {
        return INSTANCE.getWriteLockWithoutBlockingInternal(f);
    }

    /**
     * Releases a write lock on a file. First the OS lock is removed, then the process-wide write
     * lock.
     *
     * @param f the file on which to lock
     * @throws IOException if there is an error accessing the file
     */
    public static void releaseWriteLock(File f) throws IOException {
        INSTANCE.releaseWriteLockInternal(f);
    }

    private void getReadLockInternal(File f) throws IOException {
        if (!f.exists()) {
            throw new IllegalStateException("Attempted read lock on nonexistent file: " + f);
        }
        getLock(f).readLock().lock();

        AtomicInteger readerCount = getReaderCount(f);
        synchronized (readerChannels) {
            if (readerCount.incrementAndGet() == 1) {
                // First reader - must lock file in file system.
                FileChannel channel = FileChannel.open(f.toPath(), StandardOpenOption.READ);
                channel.lock(0L, Long.MAX_VALUE, true);
                readerChannels.put(f, channel);
            }
        }
    }

    private void releaseReadLockInternal(File f) throws IOException {
        AtomicInteger readerCount = getReaderCount(f);
        checkNotNull(readerCount, "Tried to release a read lock when never locked");

        synchronized (readerChannels) {
            if (readerCount.decrementAndGet() == 0) {
                // Last reader - must unlock file in file system.
                FileChannel channel = readerChannels.remove(f);
                channel.close();
            }
        }

        getLock(f).readLock().unlock();
    }

    private void getWriteLockOrBlockInternal(File f) throws IOException {
        getLock(f).writeLock().lock();

        f.getParentFile().mkdirs();
        FileChannel channel = FileChannel.open(f.toPath(), StandardOpenOption.CREATE,
            StandardOpenOption.WRITE);
        channel.lock();
        synchronized (writerChannels) {
            writerChannels.put(f, channel);
        }
    }

    private synchronized boolean getWriteLockWithoutBlockingInternal(File f) throws IOException {

        // See if some other thread already has the lock, if so return false.
        if (!getLock(f).writeLock().tryLock()) {
            return false;
        }

        // Try to get the OS-level lock.
        f.getParentFile().mkdirs();
        FileChannel channel = FileChannel.open(f.toPath(), StandardOpenOption.CREATE,
            StandardOpenOption.WRITE);
        FileLock fileLock = channel.tryLock();

        // If the OS-level lock is obtained, then record that fact and return true.
        if (fileLock != null) {
            writerChannels.put(f, channel);
            return true;
        }

        // If we got this far then we got the local lock but not the OS-level one.
        // In this case some other process has that lock, and we need to return false.
        // First, though, we need to relinquish the local lock so that the 2 locks are
        // always in the same state (either got both or ain't got both).
        getLock(f).writeLock().unlock();
        return false;

    }

    private void releaseWriteLockInternal(File f) throws IOException {
        synchronized (writerChannels) {
            FileChannel channel = writerChannels.remove(f);
            checkNotNull(channel, "Tried to release a write lock when never locked");
            channel.close();
        }

        getLock(f).writeLock().unlock();
    }

}
