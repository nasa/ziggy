package gov.nasa.ziggy.module;

import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.util.os.MemInfo;
import gov.nasa.ziggy.util.os.OperatingSystemType;
import gov.nasa.ziggy.worker.WorkerTaskRequestDispatcher;

/**
 * This class maintains a {@link Semaphore} that manages the amount of physical memory available for
 * external processes.
 * <p>
 * Before executing a task, the {@link WorkerTaskRequestDispatcher} should call this class to
 * acquire the necessary memory (as specified in the {@link PipelineModuleDefinition}) and call this
 * class again to release the memory once the task is complete. If insufficient memory is available,
 * the acquire() method will block until the memory becomes available.
 *
 * @author Todd Klaus
 */
public class WorkerMemoryManager {
    private static final Logger log = LoggerFactory.getLogger(WorkerMemoryManager.class);

    private static final int KILO = 1024;

    private Semaphore memorySemaphore;
    private int availableMegaBytes;

    public WorkerMemoryManager() throws Exception {
        MemInfo memInfo = OperatingSystemType.getInstance().getMemInfo();
        long physicalMemoryMegaBytes = memInfo.getTotalMemoryKB() / KILO;

        log.info("physicalMemoryMegaBytes: " + physicalMemoryMegaBytes);

        availableMegaBytes = (int) physicalMemoryMegaBytes;
        long jvmMaxHeapMegaBytes = Runtime.getRuntime().maxMemory() / (KILO * KILO);

        /*
         * jvmMaxHeapMegaBytes is unreliable if the max heap size is not explicitly set (with -Xmx).
         * If it's bigger than half of the physical memory, we assume the value is bad and just use
         * Runtime.getRuntime().totalMemory() (current heap size)
         */

        if (jvmMaxHeapMegaBytes < physicalMemoryMegaBytes / 2) {
            log.info("JVM max heap size set to jvmMaxHeapMegaBytes: " + jvmMaxHeapMegaBytes);

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
                log.info("JVM max heap size not available, using in-use bytes: jvmInUseMegaBytes: "
                    + jvmInUseMegaBytes);
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

        log.info("availableMegaBytes in memory manager pool: " + availableMegaBytes);
    }

    /**
     * @param megaBytes
     * @throws InterruptedException
     * @see java.util.concurrent.Semaphore#acquire(int)
     */
    public void acquireMemoryMegaBytes(int megaBytes) throws InterruptedException {
        if (megaBytes == 0) {
            return;
        }
        logAcquirePrediction(megaBytes);

        memorySemaphore.acquire(megaBytes);

        log.info(megaBytes + " megabytes acquired, new pool size: " + availableMemoryMegaBytes());
    }

    /**
     * @param megaBytes
     */
    private void logAcquirePrediction(int megaBytes) {
        int numAvailPermits = memorySemaphore.availablePermits();
        if (numAvailPermits < megaBytes) {
            log.info("Requesting " + megaBytes + " megabytes from pool, but only " + numAvailPermits
                + " megabytes available, " + memorySemaphore.getQueueLength()
                + " threads already waiting (will probably block)...");
        } else {
            log.info("Requesting " + megaBytes + " megabytes from pool (probably won't block)...");
        }
    }

    /**
     * @param megaBytes
     * @see java.util.concurrent.Semaphore#release(int)
     */
    public void releaseMemoryMegaBytes(int megaBytes) {
        if (megaBytes == 0) {
            return;
        }

        log.info("Releasing " + megaBytes + " megabytes from pool...");

        memorySemaphore.release(megaBytes);

        log.info(megaBytes + " megabytes released, new pool size: " + availableMemoryMegaBytes());
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
