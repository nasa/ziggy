package gov.nasa.ziggy.util.io;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
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
            releaseAllLocks();
        });
    }

    /**
     * Releases all locks held by the {@link LockManager} singleton instance. For use in testing and
     * in the shutdown hook.
     */
    public static synchronized void releaseAllLocks() {
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
     */
    public static void getReadLock(File f) {
        INSTANCE.getReadLockInternal(f);
    }

    /**
     * Releases a read lock on a file. If this is the last read lock, releases the OS lock on the
     * file.
     *
     * @param f the file on which to lock
     */
    public static void releaseReadLock(File f) {
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
     */
    public static void getWriteLockOrBlock(File f) {
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
     */
    public static boolean getWriteLockWithoutBlocking(File f) {
        return INSTANCE.getWriteLockWithoutBlockingInternal(f);
    }

    /**
     * Releases a write lock on a file. First the OS lock is removed, then the process-wide write
     * lock.
     *
     * @param f the file on which to lock
     */
    public static void releaseWriteLock(File f) {
        INSTANCE.releaseWriteLockInternal(f);
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private void getReadLockInternal(File f) {
        if (!f.exists()) {
            throw new IllegalStateException("Attempted read lock on nonexistent file: " + f);
        }
        getLock(f).readLock().lock();

        AtomicInteger readerCount = getReaderCount(f);
        synchronized (readerChannels) {
            try {
                if (readerCount.incrementAndGet() == 1) {
                    // First reader - must lock file in file system.
                    FileChannel channel = FileChannel.open(f.toPath(), StandardOpenOption.READ);
                    channel.lock(0L, Long.MAX_VALUE, true);
                    readerChannels.put(f, channel);
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to get read lock on file " + f.toString(),
                    e);
            }
        }
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private void releaseReadLockInternal(File f) {
        AtomicInteger readerCount = getReaderCount(f);
        checkNotNull(readerCount, "Tried to release a read lock when never locked");

        synchronized (readerChannels) {
            try {
                if (readerCount.decrementAndGet() == 0) {
                    // Last reader - must unlock file in file system.
                    FileChannel channel = readerChannels.remove(f);
                    channel.close();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(
                    "Failed to release read lock on file  " + f.toString(), e);
            }
        }

        getLock(f).readLock().unlock();
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private void getWriteLockOrBlockInternal(File f) {
        getLock(f).writeLock().lock();
        try {
            f.getParentFile().mkdirs();
            FileChannel channel = FileChannel.open(f.toPath(), StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);
            channel.lock();
            synchronized (writerChannels) {
                writerChannels.put(f, channel);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to get write lock on file " + f.toString(), e);
        }
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private synchronized boolean getWriteLockWithoutBlockingInternal(File f) {

        // See if some other thread already has the lock, if so return false.
        if (!getLock(f).writeLock().tryLock()) {
            return false;
        }

        // Try to get the OS-level lock.
        f.getParentFile().mkdirs();
        try {
            FileChannel channel = FileChannel.open(f.toPath(), StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);
            FileLock fileLock = channel.tryLock();

            // If the OS-level lock is obtained, then record that fact and return true.
            if (fileLock != null) {
                writerChannels.put(f, channel);
                return true;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to get write lock on file " + f.toString(), e);
        }

        // If we got this far then we got the local lock but not the OS-level one.
        // In this case some other process has that lock, and we need to return false.
        // First, though, we need to relinquish the local lock so that the 2 locks are
        // always in the same state (either got both or ain't got both).
        getLock(f).writeLock().unlock();
        return false;
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private void releaseWriteLockInternal(File f) {
        synchronized (writerChannels) {
            FileChannel channel = writerChannels.remove(f);
            checkNotNull(channel, "Tried to release a write lock when never locked");
            try {
                channel.close();
            } catch (IOException e) {
                throw new UncheckedIOException(
                    "Failed to release write lock on file " + f.toString(), e);
            }
        }

        getLock(f).writeLock().unlock();
    }
}
