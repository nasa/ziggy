package gov.nasa.ziggy.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.step.TaskConfiguration;
import gov.nasa.ziggy.pipeline.step.hdf5.Hdf5AlgorithmInterface;
import gov.nasa.ziggy.pipeline.step.subtask.SubtaskUtils;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.io.Persistable;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;
import gov.nasa.ziggy.util.os.OperatingSystemType;
import gov.nasa.ziggy.util.os.ProcInfo;

/**
 * Monitors the memory consumption of a specified process as a function of time. The process ID can
 * be changed at runtime. This permits a single instance of {@link ProcessMemoryMonitor} to track
 * the memory usage of an algorithm and its before and after methods.
 *
 * @author PT
 */
public class ProcessMemoryMonitor implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ProcessMemoryMonitor.class);

    // Although these aren't used here, this is a good home for them.
    public static final boolean DEFAULT_MEMORY_MONITOR_ENABLED = false;
    public static final double DEFAULT_MEMORY_MONITOR_INTERVAL_SECONDS = 60;

    private static final String SUBTASK_MEMORY_FILE_NAME_SUFFIX = "-mem-usage.h5";
    private static final String SUBTASK_MAX_MEMORY_FILE_NAME_SUFFIX = "-max-mem-usage.h5";
    private static final Pattern SUBTASK_MAX_MEMORY_FILE_PATTERN = Pattern
        .compile(SubtaskUtils.SUBTASK_DIR_REGEXP + SUBTASK_MAX_MEMORY_FILE_NAME_SUFFIX);

    private ProcInfo procInfo;
    private ScheduledThreadPoolExecutor monitoringThread;
    private final MemorySampleSeries memorySampleSeries = new MemorySampleSeries();
    private final long sampleIntervalMillis;
    private final String taskDirName;
    private final int subtaskIndex;
    private long processId;
    private final boolean enabled;

    private ProcessMemoryMonitor(String taskDirName, int subtaskIndex, long sampleIntervalMillis,
        boolean enabled) {
        this.taskDirName = taskDirName;
        this.subtaskIndex = subtaskIndex;
        this.sampleIntervalMillis = sampleIntervalMillis;
        this.enabled = enabled;
    }

    /** Returns a new and configured instance of {@link ProcessMemoryMonitor}. */
    public static ProcessMemoryMonitor newInstance(String taskDirName, int subtaskIndex) {
        TaskConfiguration taskConfiguration = TaskConfiguration
            .deserialize(DirectoryProperties.taskDataDir().resolve(taskDirName).toFile());
        long sleepMillis = (long) (taskConfiguration.getMemoryMonitorIntervalSeconds() * 1_000);

        return new ProcessMemoryMonitor(taskDirName, subtaskIndex, sleepMillis,
            taskConfiguration.isMemoryMonitorEnabled());
    }

    // Package scoped for test purposes.
    ProcInfo procInfo(long processId) {
        return OperatingSystemType.newInstance().getProcInfo(processId);
    }

    private void startMonitoring() {
        if (!isEnabled()) {
            return;
        }
        procInfo = procInfo(processId);
        log.info("Start monitoring process {} at {} second intervals", procInfo.getPid(),
            sampleIntervalMillis / 1_000L);
        monitoringThread = new ScheduledThreadPoolExecutor(1);
        monitoringThread.scheduleWithFixedDelay(this, 0, sampleIntervalMillis,
            TimeUnit.MILLISECONDS);
        ZiggyShutdownHook.addShutdownHook(() -> {
            monitoringThread.shutdownNow();
        });
    }

    public void stopMonitoring() {
        if (!isEnabled()) {
            return;
        }
        if (monitoringThread != null) {
            monitoringThread.shutdownNow();
            log.info("Monitoring halted for process {}", procInfo.getPid());
        }
    }

    /** Switch monitor to track a new process. */
    public void updateProcessId(long newPid) {
        if (!isEnabled()) {
            return;
        }
        stopMonitoring();
        processId = newPid;
        startMonitoring();
    }

    @Override
    public void run() {
        MemorySample memorySample = new MemorySample(SystemProxy.currentTimeMillis(),
            procInfo.getMemoryBytes());

        // Skip cases where no sample was obtained.
        if (memorySample.getMemoryUsageBytes() < 0) {
            return;
        }
        memorySampleSeries.addMemorySample(memorySample);
    }

    /** Writes the memory sample files for the current subtask. */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public void writeMemorySamples() {
        if (!isEnabled()) {
            return;
        }

        Path sampleDir = DirectoryProperties.runDir().resolve(taskDirName);
        String subtaskString = SubtaskUtils.subtaskDirName(subtaskIndex);
        Path memorySamplesFile = sampleDir.resolve(subtaskString + SUBTASK_MEMORY_FILE_NAME_SUFFIX);
        Path maxMemorySampleFile = sampleDir
            .resolve(subtaskString + SUBTASK_MAX_MEMORY_FILE_NAME_SUFFIX);
        Hdf5AlgorithmInterface hdf5AlgorithmInterface = new Hdf5AlgorithmInterface();
        try {
            Files.createDirectories(sampleDir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        log.info("Writing memory samples for subtask {} to file {}...", subtaskString,
            memorySamplesFile.toString());
        hdf5AlgorithmInterface.writeFile(memorySamplesFile.toFile(), memorySampleSeries, false);
        log.info("Writing memory samples for subtask {} to file {}...done", subtaskString,
            memorySamplesFile.toString());
        log.info("Writing max memory sample for subtask {} to file {}...", subtaskString,
            maxMemorySampleFile.toString());
        hdf5AlgorithmInterface.writeFile(maxMemorySampleFile.toFile(),
            memorySampleSeries.getMaxMemorySample(taskDirName, subtaskIndex), false);
        log.info("Writing max memory sample for subtask {} to file {}...done", subtaskString,
            maxMemorySampleFile.toString());
    }

    /**
     * Reads and sorts the max memory sample values for a collection of tasks. The returned non-null
     * {@link List} is in descending order. May return an empty list if memory monitoring was
     * disabled at the time this task was run.
     */
    public static List<MaxMemorySample> maxMemorySamplesDescendingOrder(
        Collection<PipelineTask> pipelineTasks) {
        List<MaxMemorySample> maxMemorySamples = new ArrayList<>();
        Hdf5AlgorithmInterface hdf5AlgorithmInterface = new Hdf5AlgorithmInterface();
        for (PipelineTask pipelineTask : pipelineTasks) {
            String taskDirName = pipelineTask.taskBaseName();

            // If the run directory doesn't exist, then memory samples were definitely not taken.
            if (!Files.isDirectory(DirectoryProperties.runDir().resolve(taskDirName))) {
                continue;
            }
            log.info("Reading max memory samples for task {}...", pipelineTask.getId());
            Set<Path> subtaskMaxMemorySamples = ZiggyFileUtils.listFiles(
                DirectoryProperties.runDir().resolve(taskDirName),
                Set.of(SUBTASK_MAX_MEMORY_FILE_PATTERN), null);
            for (Path subtaskMaxMemorySample : subtaskMaxMemorySamples) {
                MaxMemorySample maxMemorySample = new MaxMemorySample(0, 0, null);
                hdf5AlgorithmInterface.readFile(subtaskMaxMemorySample.toFile(), maxMemorySample,
                    false);
                maxMemorySamples.add(maxMemorySample);
            }
            log.info("Reading max memory samples for task {}...done", pipelineTask.getId());
        }
        return maxMemorySamples.stream().sorted().toList();
    }

    public MemorySampleSeries getMemorySampleSeries() {
        return memorySampleSeries;
    }

    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Container class for timestamped samples of memory usage. This class gets persisted to HDF5
     * for later use by analysis tools.
     */
    public static final class MemorySampleSeries implements Persistable {

        private final List<MemorySample> memorySamples = new ArrayList<>();

        public void addMemorySample(MemorySample memorySample) {
            memorySamples.add(memorySample);
        }

        public List<MemorySample> getMemorySamples() {
            return memorySamples;
        }

        public MaxMemorySample getMaxMemorySample(String taskDirName, int subtaskIndex) {
            MemorySample maxSample = null;
            for (MemorySample memorySample : memorySamples) {
                if (maxSample == null) {
                    maxSample = memorySample;
                    continue;
                }
                if (memorySample.getMemoryUsageBytes() > maxSample.getMemoryUsageBytes()) {
                    maxSample = memorySample;
                }
            }
            long pipelineTaskId = new PipelineTask.TaskBaseNameMatcher(taskDirName).taskId();
            return new MaxMemorySample(pipelineTaskId, subtaskIndex, maxSample);
        }
    }

    /** A single timestamped sample of memory usage. */
    public static final class MemorySample implements Persistable {

        private final long timestamp;
        private final long memoryUsageBytes;

        public MemorySample() {
            timestamp = -1;
            memoryUsageBytes = -1;
        }

        public MemorySample(long timestamp, long memoryUsageBytes) {
            this.timestamp = timestamp;
            this.memoryUsageBytes = memoryUsageBytes;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public long getMemoryUsageBytes() {
            return memoryUsageBytes;
        }

        @Override
        public int hashCode() {
            return Objects.hash(memoryUsageBytes, timestamp);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            MemorySample other = (MemorySample) obj;
            return memoryUsageBytes == other.memoryUsageBytes && timestamp == other.timestamp;
        }
    }

    /**
     * {@link} Persistable class that holds the {@link MemorySample} that represents the maximum
     * memory usage by a subtask. Written to a separate HDF5 file so that later analyses by Ziggy
     * don't need to reload an entire file of memory samples just to find the one that represents
     * the max usage for a subtask. The class also contains the task ID and the subtask index, in
     * order to allow aggregation of max memory samples across multiple tasks and subtasks.
     * <p>
     * NB, the comparator for this class returns samples in descending order of memory usage. This
     * allows a straightforward way to pull out the N cases with the maximum memory usage (if sorted
     * in ascending order you'd need to look at the end of the list).
     */
    public static class MaxMemorySample implements Persistable, Comparable<MaxMemorySample> {

        private final long pipelineTaskId;
        private final int subtaskIndex;
        private final MemorySample maxMemorySample;

        public MaxMemorySample() {
            pipelineTaskId = -1;
            subtaskIndex = -1;
            maxMemorySample = new MemorySample();
        }

        public MaxMemorySample(long pipelineTaskId, int subtaskIndex,
            MemorySample maxMemorySample) {
            this.pipelineTaskId = pipelineTaskId;
            this.subtaskIndex = subtaskIndex;
            this.maxMemorySample = maxMemorySample;
        }

        public long getPipelineTaskId() {
            return pipelineTaskId;
        }

        public int getSubtaskIndex() {
            return subtaskIndex;
        }

        public MemorySample getMaxMemorySample() {
            return maxMemorySample;
        }

        @Override
        public int compareTo(MaxMemorySample o) {
            return Long.signum(Long.compare(o.maxMemorySample.getMemoryUsageBytes(),
                maxMemorySample.getMemoryUsageBytes()));
        }

        @Override
        public int hashCode() {
            return Objects.hash(maxMemorySample, pipelineTaskId, subtaskIndex);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            MaxMemorySample other = (MaxMemorySample) obj;
            return Objects.equals(maxMemorySample, other.maxMemorySample)
                && pipelineTaskId == other.pipelineTaskId && subtaskIndex == other.subtaskIndex;
        }
    }
}
