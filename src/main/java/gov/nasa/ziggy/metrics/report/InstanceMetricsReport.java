package gov.nasa.ziggy.metrics.report;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.StandardXYBarPainter;
import org.jfree.chart.renderer.xy.XYBarRenderer;
import org.jfree.data.statistics.BoxAndWhiskerCalculator;
import org.jfree.data.statistics.DefaultBoxAndWhiskerCategoryDataset;
import org.jfree.data.statistics.HistogramDataset;
import org.jfree.data.statistics.HistogramType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.IntervalMetric;
import gov.nasa.ziggy.metrics.Metric;

/**
 * This class walks a task file directory tree looking for metrics-0.ser files, reads them, and
 * generates a summary report of their contents.
 *
 * @author Todd Klaus
 */
public class InstanceMetricsReport {
    private static final Logger log = LoggerFactory.getLogger(InstanceMetricsReport.class);

    private static final int CHART_HEIGHT = 500;
    private static final int CHART_WIDTH = 700;
    private static final int NUM_BINS = 100;
    private static final int TOP_N_INSTANCE = 20;
    private static final int TOP_N_TASKS = 10;

    private static final String METRICS_FILE_NAME = "metrics-0.ser";

    /**
     * Top-level directory that contains the task files. Assumes that this directory contains all of
     * the task directories, which in turn contain all of the sub-task directories
     */
    private File rootDirectory = null;

    /**
     * Name of the metric that represents the total time used by the controller. Default is
     * pipeline.module.executeAlgorithm.matlab.controller.execTime, the metric added by the MATLAB
     * code generator. This metric is used for the "Top 10" list and the pie chart breakdown
     */
    private final String totalTimeMetricName = "pipeline.module.executeAlgorithm.matlab.controller.execTime";

    // Map<metricName,rollupMetric>
    private final Map<String, Metric> instanceMetrics = new HashMap<>();

    // Map<taskFileDirname,Map<metricName,rollupMetric>
    private final Map<String, Map<String, Metric>> taskMetricsMap = new HashMap<>();

    private final TopNList instanceTopNList = new TopNList(TOP_N_INSTANCE);

    // Map<taskDirName,List<execTime>> - Complete list of exec times, by sky group
    private final Map<String, List<Double>> subTaskExecTimesByTask = new HashMap<>();

    private final List<Double> subTaskExecTimes = new ArrayList<>(200000);

    private PdfRenderer instancePdfRenderer;
    private PdfRenderer taskPdfRenderer;

    public InstanceMetricsReport(File rootDirectory) {
        if (rootDirectory == null || !rootDirectory.isDirectory()) {
            throw new IllegalArgumentException(
                "rootDirectory does not exist or is not a directory: " + rootDirectory);
        }
        this.rootDirectory = rootDirectory;
    }

    public void generateReport() throws Exception {
        instancePdfRenderer = new PdfRenderer(
            new File(rootDirectory, "metrics-" + rootDirectory.getName() + "-instance-rpt.pdf"));
        taskPdfRenderer = new PdfRenderer(
            new File(rootDirectory, "metrics-" + rootDirectory.getName() + "-task-rpt.pdf"));

        instancePdfRenderer.printText("Metrics Report for " + rootDirectory.getName(),
            PdfRenderer.titleFont);
        taskPdfRenderer.printText("Metrics Report for " + rootDirectory.getName(),
            PdfRenderer.titleFont);

        parseFiles();

        log.info("Instance Metrics");
        dump(instanceMetrics);

        dumpTopTen(instancePdfRenderer, "Top " + TOP_N_INSTANCE + " for instance: ",
            instanceTopNList);

        JFreeChart histogram = generateHistogram("instance", subTaskExecTimes);

        if (histogram != null) {
            chart2Png(histogram,
                new File(rootDirectory, "exec-time-hist-" + rootDirectory.getName() + ".png"));
            instancePdfRenderer.printChart(histogram, CHART_WIDTH, CHART_HEIGHT);
        } else {
            instancePdfRenderer.printText("No data points available");
        }

        JFreeChart boxNWhiskers = generateBoxAndWhiskers();
        chart2Png(boxNWhiskers, new File(rootDirectory, "exec-time-bnw-instance.png"));

        instancePdfRenderer.printChart(boxNWhiskers, CHART_WIDTH, CHART_HEIGHT);

        instancePdfRenderer.close();
        taskPdfRenderer.close();
    }

    private void chart2Png(JFreeChart chart, File outputPngFile) throws Exception {
//        FileOutputStream fos = new FileOutputStream(outputPngFile);
//        BufferedOutputStream bos = new BufferedOutputStream(fos);
//        ChartUtilities.writeChartAsPNG(bos, chart, 800, 600);
//        bos.close();
//        fos.close();
    }

    private void parseFiles() throws Exception {
        File[] taskDirs = rootDirectory
            .listFiles((FileFilter) f -> f.getName().contains("-matlab-") && f.isDirectory());

        for (File taskDir : taskDirs) {
            log.info("Processing: " + taskDir);

            String taskDirName = taskDir.getName();
            Map<String, Metric> taskMetrics = taskMetricsMap.get(taskDirName);
            TopNList taskTopNList = new TopNList(TOP_N_TASKS);

            if (taskMetrics == null) {
                taskMetrics = new HashMap<>();
                taskMetricsMap.put(taskDirName, taskMetrics);
            }

            File[] subTaskDirs = taskDir
                .listFiles((FileFilter) pathname -> pathname.getName().startsWith("st-")
                    && pathname.isDirectory());

            if (subTaskDirs != null) {
                log.info("Found " + subTaskDirs.length + " sub-task directories");
            } else {
                log.info("No sub-task directories found");
            }

            for (File subTaskDir : subTaskDirs) {
                File subTaskMetricsFile = new File(subTaskDir, METRICS_FILE_NAME);

                if (subTaskMetricsFile.exists()) {
                    Map<String, Metric> subTaskMetrics = Metric
                        .loadMetricsFromSerializedFile(subTaskMetricsFile);

                    for (Metric metric : subTaskMetrics.values()) {
                        // merge this metric into the instance metrics
                        merge(metric, instanceMetrics);
                        // merge this metric into the task metrics
                        merge(metric, taskMetrics);

                        if (metric.getName().equals(totalTimeMetricName)) {
                            IntervalMetric totalTimeMetric = (IntervalMetric) metric;
                            int execTime = (int) totalTimeMetric.getAverage();
                            instanceTopNList.add(execTime,
                                taskDirName + "/" + subTaskDir.getName());
                            taskTopNList.add(execTime, taskDirName + "/" + subTaskDir.getName());
                            addExecTime(taskDirName, totalTimeMetric.getAverage());
                        }
                    }
                } else {
                    log.warn("No metrics file found in: " + subTaskDir);
                }
            }

            log.info("Metrics for: " + taskDirName);
            dumpTopTen(taskPdfRenderer, "Top " + TOP_N_TASKS + " for task: " + taskDirName,
                taskTopNList);

            List<Double> taskExecTimes = subTaskExecTimesByTask.get(taskDirName);
            JFreeChart histogram = generateHistogram(taskDirName, taskExecTimes);

            if (histogram != null) {
                chart2Png(histogram,
                    new File(rootDirectory, "exec-time-hist-" + taskDirName + ".png"));
                taskPdfRenderer.printChart(histogram, CHART_WIDTH, CHART_HEIGHT);
                taskPdfRenderer.newPage();
            } else {
                taskPdfRenderer.printText("No data points available");
            }
        }
    }

    private void addExecTime(String taskDirName, double execTime) {
        List<Double> timesForTask = subTaskExecTimesByTask.get(taskDirName);
        if (timesForTask == null) {
            timesForTask = new ArrayList<>(5000);
            subTaskExecTimesByTask.put(taskDirName, timesForTask);
        }

        double timeHours = execTime / 1000.0 / 3600.0; // convert to hours
        timesForTask.add(timeHours);
        subTaskExecTimes.add(timeHours);
    }

    private double[] listToArray(List<Double> list) {
        if (list == null || list.size() == 0) {
            return new double[0];
        }

        double[] array = new double[list.size()];
        int index = 0;

        for (Double value : list) {
            array[index++] = value;
        }

        return array;
    }

    private JFreeChart generateHistogram(String label, List<Double> execTimes) throws Exception {
        if (execTimes == null || execTimes.size() == 0) {
            return null;
        }

        double[] values = listToArray(execTimes);

        HistogramDataset dataset = new HistogramDataset();
        dataset.setType(HistogramType.RELATIVE_FREQUENCY);
        dataset.addSeries("execTime", values, NUM_BINS);

        JFreeChart chart = ChartFactory.createHistogram("Algorithm Run-time (" + label + ")",
            "execTime (hours)", "Number of Sub-tasks", dataset, PlotOrientation.VERTICAL, true,
            true, false);
        XYPlot plot = (XYPlot) chart.getPlot();
        plot.setDomainPannable(true);
        plot.setRangePannable(true);
        plot.setForegroundAlpha(0.85f);
        NumberAxis yAxis = (NumberAxis) plot.getRangeAxis();
        yAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
        XYBarRenderer renderer = (XYBarRenderer) plot.getRenderer();
        renderer.setDrawBarOutline(false);
        // flat bars look best...
        renderer.setBarPainter(new StandardXYBarPainter());
        renderer.setShadowVisible(false);

        return chart;
    }

    private JFreeChart generateBoxAndWhiskers() throws Exception {
        DefaultBoxAndWhiskerCategoryDataset dataset = new DefaultBoxAndWhiskerCategoryDataset();

        Set<String> taskNames = subTaskExecTimesByTask.keySet();
        for (String taskName : taskNames) {
            log.info("taskDirName = " + taskName);
            List<Double> execTimesForTask = subTaskExecTimesByTask.get(taskName);
            dataset.add(BoxAndWhiskerCalculator.calculateBoxAndWhiskerStatistics(execTimesForTask),
                taskName, taskName);
        }

        JFreeChart chart = ChartFactory.createBoxAndWhiskerChart(
            "Run Time Distribution by Sky Group", "Sky Group", "Run Time (hours)", dataset, false);

        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setDomainGridlinesVisible(true);

        return chart;
    }

    private void merge(Metric metricToMerge, Map<String, Metric> mergeDestination) {
        String metricName = metricToMerge.getName();
        Metric existingMetric = mergeDestination.get(metricName);
        if (existingMetric == null) {
            // first time seeing this metric
            mergeDestination.put(metricName, metricToMerge.makeCopy());
        } else {
            existingMetric.merge(metricToMerge);
        }
    }

    private void dump(Map<String, Metric> metrics) {
        for (Metric metric : metrics.values()) {
            log.info(metric.getName() + ": " + metric.toString());
        }
    }

    private void dumpTopTen(PdfRenderer pdfRenderer, String title, TopNList topTenList)
        throws Exception {
        List<TopNListElement> list = topTenList.getList();

        pdfRenderer.printText("Top Stragglers", PdfRenderer.h1Font);
        pdfRenderer.printText(title);

        int index = 1;
        for (TopNListElement element : list) {
            String duration = DurationFormatUtils.formatDuration(element.getValue(), "HH:mm:ss");
            pdfRenderer.printText(index + " - " + element.getLabel() + ": " + duration);
            index++;
        }
    }

}
