package gov.nasa.ziggy.module;

import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.os.MemInfo;
import gov.nasa.ziggy.util.os.OperatingSystemType;
import gov.nasa.ziggy.worker.TaskExecutor;

/**
 * This class maintains a {@link Semaphore} that manages the amount of physical memory available for
 * external processes.
 * <p>
 * Before executing a task, the {@link TaskExecutor} should call this class to acquire the necessary
 * memory (as specified in the {@link PipelineModuleDefinition}) and call this class again to
 * release the memory once the task is complete. If insufficient memory is available, the acquire()
 * method will block until the memory becomes available.
 *
 * @author Todd Klaus
 */
public class WorkerMemoryManager {
    private static final Logger log = LoggerFactory.getLogger(WorkerMemoryManager.class);

    private static final int KILO = 1024;

    private Semaphore memorySemaphore;
    private int availableMegaBytes;

    public WorkerMemoryManager() {
        MemInfo memInfo = OperatingSystemType.newInstance().getMemInfo();
        long physicalMemoryMegaBytes = memInfo.getTotalMemoryKB() / KILO;

        log.info("physicalMemoryMegaBytes={}", physicalMemoryMegaBytes);

        availableMegaBytes = (int) physicalMemoryMegaBytes;
        long jvmMaxHeapMegaBytes = Runtime.getRuntime().maxMemory() / (KILO * KILO);

        /*
         * jvmMaxHeapMegaBytes is unreliable if the max heap size is not explicitly set (with -Xmx).
         * If it's bigger than half of the physical memory, we assume the value is bad and just use
         * Runtime.getRuntime().totalMemory() (current heap size)
         */

        if (jvmMaxHeapMegaBytes < physicalMemoryMegaBytes / 2) {
            log.info("JVM max heap size set to {}", jvmMaxHeapMegaBytes);
            availableMegaBytes -= jvmMaxHeapMegaBytes;
        } else {
            /*
             * If the max heap size is not set, the best we can do is use the current heap size.
             * This may result in an available pool that exceeds the amount of physical memory,
             * which in turn *could* result in swapping. In practice, however, we always explicitly
             * set the max heap size (-Xmx)
             */
            long jvmInUseMegaBytes = Runtime.getRuntime().totalMemory() / (KILO * KILO);

            if (jvmInUseMegaBytes < physicalMemoryMegaBytes / 2) {
                log.info("JVM max heap size not available, using in-use heap {}",
                    jvmInUseMegaBytes);
                availableMegaBytes -= jvmInUseMegaBytes;
            } else {
                log.info("JVM heap size not available, not accounted for in pool");
            }
        }

        initSemaphore();
    }

    public WorkerMemoryManager(int availableMegaBytes) {
        this.availableMegaBytes = availableMegaBytes;

        initSemaphore();
    }

    private void initSemaphore() {
        memorySemaphore = new Semaphore(availableMegaBytes, true);

        log.info("Memory manager pool has {} MB", availableMegaBytes);
    }

    /**
     * @see java.util.concurrent.Semaphore#acquire(int)
     */
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    public void acquireMemoryMegaBytes(int megaBytes) {
        if (megaBytes == 0) {
            return;
        }
        logAcquirePrediction(megaBytes);

        try {
            memorySemaphore.acquire(megaBytes);
            log.info("Acquired {} MB, new pool size is {}", megaBytes, availableMemoryMegaBytes());
        } catch (InterruptedException ignored) {
            // If we got here, it means that a worker thread was waiting for Java heap to become
            // available but that thread was interrupted. It is therefore no longer waiting for
            // Java heap! But all the other threads still need the memory manager to function,
            // so we swallow this exception.
        }
    }

    private void logAcquirePrediction(int megaBytes) {
        int numAvailPermits = memorySemaphore.availablePermits();
        if (numAvailPermits < megaBytes) {
            log.info(
                "Requesting {} MB from pool, but only {} MB available, {} threads already waiting (will probably block)",
                megaBytes, numAvailPermits, memorySemaphore.getQueueLength());
        } else {
            log.info("Requesting {} MB from pool (probably won't block)", megaBytes);
        }
    }

    /**
     * @see java.util.concurrent.Semaphore#release(int)
     */
    public void releaseMemoryMegaBytes(int megaBytes) {
        if (megaBytes == 0) {
            return;
        }

        log.info("Releasing {} MB from pool", megaBytes);

        memorySemaphore.release(megaBytes);

        log.info("Released {} MB, new pool size is {} MB", megaBytes, availableMemoryMegaBytes());
    }

    /**
     * @return
     * @see java.util.concurrent.Semaphore#availablePermits()
     */
    public int availableMemoryMegaBytes() {
        return memorySemaphore.availablePermits();
    }

    /**
     * @return
     * @see java.util.concurrent.Semaphore#tryAcquire()
     */
    public boolean tryAcquireMegaBytes(int megaBytes) {
        if (megaBytes == 0) {
            return true;
        }

        logAcquirePrediction(megaBytes);

        return memorySemaphore.tryAcquire(megaBytes);
    }
}
