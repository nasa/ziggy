package gov.nasa.ziggy.metrics;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all metrics.
 *
 * @author Todd Klaus
 */
public abstract class Metric implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(Metric.class);

    private static final long serialVersionUID = -2723751150597278137L;

    private static Logger metricsLogger = LoggerFactory.getLogger("metrics.logger");

    private static ConcurrentMap<String, Metric> globalMetrics = new ConcurrentHashMap<>();
    private static ThreadLocal<Map<String, Metric>> threadMetrics = new ThreadLocal<>();

    protected String name = null;

    /**
     * get the name of this metric
     *
     * @return
     */
    public String getName() {
        return name;
    }

    /**
     * @return Returns the map of global metrics.
     */
    public static Map<String, Metric> getGlobalMetricsSnapshot() {
        Map<String, Metric> metricsCopy = new HashMap<>();

        for (String metricName : globalMetrics.keySet()) {
            Metric metricCopy = globalMetrics.get(metricName).makeCopy();
            metricsCopy.put(metricName, metricCopy);
        }
        return metricsCopy;
    }

    /**
     * Returns the map of thread metrics for the calling thread. May be null if thread metrics have
     * not been enabled for the calling thread.
     *
     * @return Map of metrics for the calling thread
     */
    public static Map<String, Metric> getThreadMetrics() {
        return threadMetrics.get();
    }

    /**
     * Iterate over the metric names
     *
     * @return
     */
    public static Iterator<String> metricsIterator() {
        return globalMetrics.keySet().iterator();
    }

    /**
     * Iterate over the metric names that start with the specified String
     *
     * @return
     */
    public static Iterator<String> metricsIterator(String startsWith) {
        return new StartsWithIterator(globalMetrics.keySet().iterator(), startsWith);
    }

    /**
     * clear the map
     */
    public static void clear() {
        globalMetrics = new ConcurrentHashMap<>();
        threadMetrics = new ThreadLocal<>();
    }

    /**
     * log all metrics to the log4j Logger
     */
    public static void log() {
        long now = System.currentTimeMillis();
        metricsLogger.info("SNAPSHOT-START@" + now);

        Iterator<String> it = Metric.metricsIterator();
        logWithIterator(it);

        metricsLogger.info("SNAPSHOT-END@" + now);
    }

    /**
     * Dump all metrics to stdout
     */
    public static void dump() {
        Metric.dump(new PrintWriter(System.out));
    }

    /**
     * Dump all metrics to specified writer
     */
    public static void dump(PrintWriter writer) {
        Set<String> names = globalMetrics.keySet();

        for (String name : names) {
            Metric metric = Metric.getGlobalMetric(name);
            StringBuilder bldr = new StringBuilder(128);
            bldr.append(System.currentTimeMillis()).append(',');
            metric.toLogString(bldr);
            writer.println(bldr.toString());
        }
    }

    /**
     * @see toLogString
     * @return
     */
    public final String getLogString() {
        StringBuilder bldr = new StringBuilder(64);
        toLogString(bldr);
        return bldr.toString();
    }

    /**
     * Must be implemented by subclasses. Should return a String representation of the metrics which
     * includes the type in the following comma separated format: (type),(metric values ...)
     * <p>
     * For example, a ValueMetric (min, max, count, sum) might look like this:
     * <p>
     * V,0,100,42,12
     * <p>
     * Writes the string getLogString() to the specified string builder.
     *
     * @param bldr non-null
     */
    public abstract void toLogString(StringBuilder bldr);

    /**
     * Persist the current set of global metrics to a file using Java serialization.
     * <p>
     * Used to transfer metrics collected in a sub-process to the parent process.
     *
     * @param path
     * @throws IOException
     */
    public static void persist(String path) throws IOException {
        File file = new File(path);

        if (file.isDirectory()) {
            throw new IllegalArgumentException("Specified file is a directory: " + file);
        }

        try (ObjectOutputStream output = new ObjectOutputStream(
            new BufferedOutputStream(new FileOutputStream(file)))) {
            output.writeObject(globalMetrics);
        }
    }

    /**
     * Load a set of metrics from a file using Java serialization. The loaded metrics are then
     * merged with the current set of metrics.
     * <p>
     * Used to transfer metrics collected in a sub-process to the parent process.
     *
     * @param path
     * @throws Exception
     */
    public static void merge(String path) throws Exception {
        File file = new File(path);

        if (file.isDirectory()) {
            throw new IllegalArgumentException("Specified file is a directory: " + file);
        }

        Map<String, Metric> metricsToMerge = loadMetricsFromSerializedFile(file);

        for (String metricName : metricsToMerge.keySet()) {
            Metric metricToMerge = metricsToMerge.get(metricName);

            log.debug("merge: metricToMerge=" + metricToMerge);

            Metric existingGlobalMetric = globalMetrics.get(metricName);
            if (existingGlobalMetric != null) {
                log.debug("merge: existingGlobalMetric(BEFORE)=" + existingGlobalMetric);
                existingGlobalMetric.merge(metricToMerge);
                log.debug("merge: existingGlobalMetric(AFTER)=" + existingGlobalMetric);
            } else {
                log.debug("No existingGlobalMetric exists, adding");
                globalMetrics.put(metricName, metricToMerge.makeCopy());
            }

            if (Metric.threadMetricsEnabled()) {
                Metric existingThreadMetric = Metric.getThreadMetric(metricName);
                if (existingThreadMetric != null) {
                    log.debug("merge: existingThreadMetric(BEFORE)=" + existingThreadMetric);
                    existingThreadMetric.merge(metricToMerge);
                    log.debug("merge: existingThreadMetric(AFTER)=" + existingThreadMetric);
                } else {
                    log.debug("No existingThreadMetric exists, adding");
                    Metric.addNewThreadMetric(metricToMerge.makeCopy());
                }
            }
        }
    }

    /**
     * Load a Metrics map from a serialized (*.ser) file.
     *
     * @param file
     * @return
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Metric> loadMetricsFromSerializedFile(File file) throws Exception {
        try (ObjectInputStream input = new ObjectInputStream(
            new BufferedInputStream(new FileInputStream(file)))) {
            return (Map<String, Metric>) input.readObject();
        }
    }

    /**
     * Merge another metric (typically from a subprocess) with an existing metric.
     *
     * @param other
     */
    public abstract void merge(Metric other);

    /**
     * Make a copy of the Metric
     *
     * @return
     */
    public abstract Metric makeCopy();

    /**
     * Constructor. Subclass constructors must call setName().
     */
    protected Metric() {
    }

    protected void setName(String name) {
        if (name == null) {
            throw new NullPointerException("Metric name must not be null!");
        }
        this.name = name;
    }

    /**
     * static accessor to get a metric by name
     *
     * @param name
     * @return
     */
    protected static Metric getGlobalMetric(String name) {
        return globalMetrics.get(name);
    }

    /**
     * static accessor to get a metric by name
     *
     * @param name
     * @return
     */
    protected static Metric getThreadMetric(String name) {
        Map<String, Metric> threadMap = threadMetrics.get();

        if (threadMap != null) {
            return threadMap.get(name);
        } else {
            return null;
        }
    }

    protected static boolean threadMetricsEnabled() {
        return threadMetrics.get() != null;
    }

    /**
     * Enables collection of metrics at the thread level and initializes them.
     * <p>
     * Thread metrics are collected in a separate map from global metrics addition to the collection
     * of metrics at the JVM level (global).
     */
    public static void enableThreadMetrics() {
        Map<String, Metric> threadMap = new HashMap<>();
        threadMetrics.set(threadMap);
    }

    /**
     * Disables collection of metrics at the thread level.
     */
    public static void disableThreadMetrics() {
        threadMetrics.remove();
    }

    /**
     * Add a new global metric to the map. This uses ConcurrentMap.putIfAbsent which can be slow.
     * You might want to check if the metric is in the map beforehand.
     *
     * @param metric
     * @return this metric may not be identical to the given metric in the concurrent case.
     */
    protected static Metric addNewGlobalMetric(Metric metric) {
        if (metric != null) {
            String metricName = metric.getName();
            globalMetrics.putIfAbsent(metricName, metric);
            metric = globalMetrics.get(metricName);
        }
        return metric;
    }

    /**
     * Add a new thread metric to the map.
     *
     * @param metric
     * @return
     */
    protected static Metric addNewThreadMetric(Metric metric) {
        if (metric != null) {
            Map<String, Metric> threadMap = threadMetrics.get();
            if (threadMap != null) {
                threadMap.put(metric.getName(), metric);
            }
        }
        return metric;
    }

    /**
     * Log all metrics whose name starts with the specified prefix to the log4j Logger
     *
     * @param prefix
     */
    protected static void log(String prefix) {
        long now = System.currentTimeMillis();
        metricsLogger.info("SNAPSHOT-START@" + now);

        Iterator<String> it = Metric.metricsIterator(prefix);
        logWithIterator(it);

        metricsLogger.info("SNAPSHOT-END@" + now);
    }

    /**
     * Log all metrics whose name starts with the specified prefixes to the log4j Logger
     *
     * @param prefixes
     */
    protected static void log(List<String> prefixes) {
        long now = System.currentTimeMillis();
        metricsLogger.info("SNAPSHOT-START@" + now);

        Iterator<String> prefixIt = prefixes.iterator();
        while (prefixIt.hasNext()) {
            String prefix = prefixIt.next();
            Iterator<String> it = Metric.metricsIterator(prefix);
            logWithIterator(it);
        }

        metricsLogger.info("SNAPSHOT-END@" + now);
    }

    /**
     * Log using the specified iterator
     *
     * @param it
     */
    protected static void logWithIterator(Iterator<String> it) {
        while (it.hasNext()) {
            String name = it.next();
            Metric metric = Metric.getGlobalMetric(name);
            metricsLogger.debug(metric.getName() + ":" + metric.getLogString());
        }
    }

    /**
     * reset metric
     */
    protected abstract void reset();

    public static class StartsWithIterator implements Iterator<String> {
        private final Iterator<String> it;

        private final String startsWith;

        private String currentValue = null;

        StartsWithIterator(Iterator<String> it, String startsWith) {
            this.it = it;
            this.startsWith = startsWith;
        }

        @Override
        public boolean hasNext() {
            if (!it.hasNext()) {
                return false;
            }
            while (it.hasNext() && currentValue == null) {
                String metricName = it.next();
                if (metricName.startsWith(startsWith)) {
                    currentValue = metricName;
                }
            }
            return currentValue != null;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("this is a read-only iterator");
        }

        @Override
        public String next() {
            try {
                return currentValue;
            } finally {
                currentValue = null;
            }
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final Metric other = (Metric) obj;
        if (!Objects.equals(name, other.name)) {
            return false;
        }
        return true;
    }
}
