package gov.nasa.ziggy.metrics.report;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import gov.nasa.ziggy.metrics.IntervalMetric;
import gov.nasa.ziggy.metrics.Metric;
import gov.nasa.ziggy.module.SubtaskDirectoryIterator;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.SpotBugsUtils;

public class MatlabMetrics {
    private static final Logger log = LoggerFactory.getLogger(MatlabMetrics.class);

    private static final String MATLAB_METRICS_FILENAME = "metrics-0.ser";
    private static final String MATLAB_METRICS_CACHE_FILENAME = "metrics-cache.ser";
    private static final String MATLAB_CONTROLLER_EXEC_TIME_METRIC = "pipeline.module.executeAlgorithm.matlab.controller.execTime";

    private final File taskFilesDir;
    private final String moduleName;

    private boolean cacheResults = true;

    private boolean parsed = false;
    private DescriptiveStatistics totalTimeStats;
    private HashMap<String, DescriptiveStatistics> functionStats;
    private TopNList topTen;

    public MatlabMetrics(File taskFilesDir, String moduleName) {
        this.taskFilesDir = taskFilesDir;
        this.moduleName = moduleName;
    }

    private static final class CacheContents implements Serializable {
        private static final long serialVersionUID = 20230511L;
        public DescriptiveStatistics totalTime;
        public HashMap<String, DescriptiveStatistics> function;
        public TopNList topTen;
    }

    @SuppressFBWarnings(value = "OBJECT_DESERIALIZATION",
        justification = SpotBugsUtils.DESERIALIZATION_JUSTIFICATION)
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    public void parseFiles() {
        File cacheFile = new File(taskFilesDir, MATLAB_METRICS_CACHE_FILENAME);
        try {
            if (!parsed) {
                totalTimeStats = new DescriptiveStatistics();
                functionStats = new HashMap<>();
                topTen = new TopNList(10);

                if (cacheFile.exists()) {
                    log.info("Found cache file");
                    try (ObjectInputStream ois = new ObjectInputStream(
                        new BufferedInputStream(new FileInputStream(cacheFile)))) {
                        CacheContents cacheContents = (CacheContents) ois.readObject();

                        totalTimeStats = cacheContents.totalTime;
                        functionStats = cacheContents.function;
                        topTen = cacheContents.topTen;
                    }
                } else { // no cache
                    log.info("No cache file found, parsing files");
                    File[] taskDirs = taskFilesDir
                        .listFiles((FileFilter) f -> f.getName().startsWith(moduleName + "-")
                            && f.isDirectory());

                    for (File taskDir : taskDirs) {
                        log.info("Processing: " + taskDir);

                        SubtaskDirectoryIterator directoryIterator = new SubtaskDirectoryIterator(
                            taskDir);

                        if (directoryIterator.hasNext()) {
                            log.info("Found " + directoryIterator.numSubTasks()
                                + " sub-task directories");
                        } else {
                            log.info("No sub-task directories found");
                        }

                        while (directoryIterator.hasNext()) {
                            File subTaskDir = directoryIterator.next().getSubtaskDir();

                            log.debug("STM: " + subTaskDir);

                            File subTaskMetricsFile = new File(subTaskDir, MATLAB_METRICS_FILENAME);

                            if (subTaskMetricsFile.exists()) {
                                Map<String, Metric> subTaskMetrics = Metric
                                    .loadMetricsFromSerializedFile(subTaskMetricsFile);

                                for (String metricName : subTaskMetrics.keySet()) {
                                    if (!metricName.equals(MATLAB_CONTROLLER_EXEC_TIME_METRIC)) {
                                        Metric metric = subTaskMetrics.get(metricName);

                                        log.debug("STM: " + metricName + ": " + metric.toString());

                                        DescriptiveStatistics metricStats = functionStats
                                            .get(metricName);
                                        if (metricStats == null) {
                                            metricStats = new DescriptiveStatistics();
                                            functionStats.put(metricName, metricStats);
                                        }

                                        IntervalMetric totalTimeMetric = (IntervalMetric) metric;
                                        metricStats.addValue(totalTimeMetric.getAverage());
                                    }
                                }

                                Metric metric = subTaskMetrics
                                    .get(MATLAB_CONTROLLER_EXEC_TIME_METRIC);
                                if (metric != null) {
                                    String subTaskName = subTaskDir.getParentFile().getName() + "/"
                                        + subTaskDir.getName();

                                    IntervalMetric totalTimeMetric = (IntervalMetric) metric;
                                    double mean = totalTimeMetric.getAverage();
                                    totalTimeStats.addValue(mean);
                                    topTen.add((long) mean, subTaskName);
                                } else {
                                    log.warn("no metric found with name: "
                                        + MATLAB_CONTROLLER_EXEC_TIME_METRIC + " in:" + subTaskDir);
                                }
                            } else {
                                log.warn("No metrics file found in: " + subTaskDir);
                            }
                        }
                    }

                    if (cacheResults) {
                        try (ObjectOutputStream oos = new ObjectOutputStream(
                            new BufferedOutputStream(new FileOutputStream(cacheFile)))) {
                            CacheContents cache = new CacheContents();
                            cache.totalTime = totalTimeStats;
                            cache.function = functionStats;
                            cache.topTen = topTen;

                            oos.writeObject(cache);
                            oos.flush();
                        }
                    }
                }
                parsed = true;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                "IOException occurred accessing file " + cacheFile.toString(), e);
        } catch (ClassNotFoundException e) {
            // This exception can never occur. This class is the sole creator and consumer
            // of the serialized object file, hence it is guaranteed that the class that is
            // serialized is also the class that is retrieved.
            throw new AssertionError(e);
        }
    }

    public boolean isCacheResults() {
        return cacheResults;
    }

    public void setCacheResults(boolean cacheResults) {
        this.cacheResults = cacheResults;
    }

    public DescriptiveStatistics getTotalTimeStats() {
        return totalTimeStats;
    }

    public Map<String, DescriptiveStatistics> getFunctionStats() {
        return functionStats;
    }

    public TopNList getTopTen() {
        return topTen;
    }
}
