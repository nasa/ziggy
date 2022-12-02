package gov.nasa.ziggy.util;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a background monitor of heap usage. When a configured threshold of usage is reached,
 * it sends a warning notification and logs a warning message. Also, other services may query the
 * monitor to determine if memory is low and curtail services accordingly.
 */
public class MemoryMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryMonitor.class);

    private static final long GiB = 1024L * 1024L * 1024L;

    private long minHeapThreshold;
    private long pollInterval;
    private AtomicBoolean shouldStopMonitoring = new AtomicBoolean(false);
    private MemoryPoolMXBean monitorPool;

    /**
     * Creates a new instance with a desired minimum heap amount and polling interval for monitoring
     * memory.
     *
     * @param minHeapThreshold the threshold minimum amount of heap memory to detect, in bytes
     * @param pollInterval the polling interval, in milliseconds
     */
    public MemoryMonitor(long minHeapThreshold, long pollInterval) {
        this.minHeapThreshold = minHeapThreshold;
        this.pollInterval = pollInterval;

        // Find the largest memory pool to monitor.
        for (MemoryPoolMXBean poolBean : ManagementFactory.getMemoryPoolMXBeans()) {
            if (poolBean.getType() == MemoryType.HEAP && poolBean.isUsageThresholdSupported()) {
                if (monitorPool == null
                    || poolBean.getUsage().getMax() > monitorPool.getUsage().getMax()) {
                    monitorPool = poolBean;
                }
            }
        }

        // If we did not find a suitable pool, generate an error.
        if (monitorPool == null) {
            throw new IllegalArgumentException("No heap pool found supporting usage monitoring");
        }
    }

    /**
     * Tests whether memory usage exceeds the threshold.
     *
     * @return true, if memory usage exceeds the threshold
     */
    public boolean isThresholdExceeded() {
        FreeSpace space = getFreeSpace();

        // If we are under the threshold, force a GC before testing again.
        if (space.getSize() < minHeapThreshold) {
            System.gc();
            space = getFreeSpace();
        }

        return space.getSize() < minHeapThreshold;
    }

    /**
     * Starts memory monitoring.
     */
    public void startMonitoring() {
        shouldStopMonitoring.set(false);

        new Thread(() -> {
            while (!shouldStopMonitoring.get()) {
                FreeSpace space = getFreeSpace();

                String msg = String.format("free=%.3f GiB  %.1f%%", (double) space.getSize() / GiB,
                    space.getPercent());
                if (isThresholdExceeded()) {
                    LOG.error("Memory threshold exceeded: " + msg);
                    System.out.println("Memory threshold exceeded: " + msg);
                } else {
                    LOG.info("Memory usage: " + msg);
                    System.out.println("Memory usage: " + msg);
                }

                try {
                    Thread.sleep(pollInterval);
                } catch (Exception e) {
                    // Ignore.
                }
            }
        }).start();
    }

    private FreeSpace getFreeSpace() {
        MemoryUsage usage = monitorPool.getUsage();
        long free = usage.getMax() - usage.getUsed();
        return new FreeSpace(free, 100.0 * free / usage.getMax());
    }

    /**
     * Stops memory monitoring.
     */
    public void stopMonitoring() {
        shouldStopMonitoring.set(true);
    }

    /**
     * Gets the name of the memory pool being monitored.
     *
     * @return the pool name
     */
    public String getMonitoredPoolName() {
        return monitorPool.getName();
    }

    /**
     * Represents an amount of free space, both as an absolute size and as a percentage of space
     * available.
     */
    private static class FreeSpace {

        private long size;
        private double percent;

        public FreeSpace(long size, double percent) {
            this.size = size;
            this.percent = percent;
        }

        public long getSize() {
            return size;
        }

        public double getPercent() {
            return percent;
        }

    }

}
