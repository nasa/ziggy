package gov.nasa.ziggy.metrics.report;

import java.io.File;
import java.io.FileFilter;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.jfree.chart.JFreeChart;
import org.jfree.data.general.DefaultPieDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MatlabReport extends Report {
    private static final Logger log = LoggerFactory.getLogger(MatlabReport.class);
    private final String moduleName;
    private final File taskFilesDir;
    private final long instanceId;

    public MatlabReport(PdfRenderer pdfRenderer, File taskFilesDir, String moduleName,
        long instanceId) {
        super(pdfRenderer);
        this.moduleName = moduleName;
        this.taskFilesDir = taskFilesDir;
        this.instanceId = instanceId;
    }

    /**
     * Generate stacked bar chart Generate descriptive statistics and a histogram of the peak memory
     * usage for all matlab processes for all tasks for the specified module.
     *
     * @throws Exception
     */
    public void generateReport() throws Exception {
        generateExecTimeReport();
        generateMemoryReport();
    }

    private void generateExecTimeReport() throws Exception {
        MatlabMetrics matlabMetrics = new MatlabMetrics(taskFilesDir, moduleName);
        matlabMetrics.parseFiles();

        DescriptiveStatistics matlabStats = matlabMetrics.getTotalTimeStats();

        Map<String, DescriptiveStatistics> matlabFunctionStats = matlabMetrics.getFunctionStats();

        double totalTime = matlabStats.getSum();
        double otherTime = totalTime;

        DefaultPieDataset functionBreakdownDataset = new DefaultPieDataset();

        log.info("breakdown report");

        for (String metricName : matlabFunctionStats.keySet()) {
            String label = shortMetricName(metricName);

            log.info("processing metric: " + label);

            DescriptiveStatistics functionStats = matlabFunctionStats.get(metricName);
            double functionTime = functionStats.getSum();
            double fraction = functionTime / totalTime;

            if (fraction > 0.01) {
                functionBreakdownDataset.setValue(label, fraction);
            }
            otherTime -= functionTime;
        }

        double otherFraction = otherTime / totalTime;
        functionBreakdownDataset.setValue("Other", otherFraction);

        JFreeChart pie = generatePieChart("MATLAB Algorithm Breakdown", functionBreakdownDataset);

        pdfRenderer.printChart(pie, CHART2_WIDTH, CHART2_HEIGHT);

        pdfRenderer.newPage();

        HumanReadableStatistics values = millisToHumanReadable(matlabStats);
        JFreeChart execHistogram = generateHistogram("MATLAB Controller Run Time",
            "Time (" + values.getUnit() + ")", "Sub-Tasks", values.getValues(), 100);

        if (execHistogram != null) {
            pdfRenderer.printChart(execHistogram, CHART3_WIDTH, CHART3_HEIGHT);
        } else {
            pdfRenderer.printText("Histogram: No data points available");
        }

        pdfRenderer.printText(" ");

        generateSummaryTable("MATLAB Controller", matlabStats, matlabMetrics.getTopTen(),
            new TimeMillisFormat());

        pdfRenderer.newPage();
    }

    private void generateMemoryReport() throws Exception {
        File[] taskDirs = taskFilesDir
            .listFiles((FileFilter) f -> f.getName().contains(moduleName + "-") && f.isDirectory());

        DescriptiveStatistics memoryStats = new DescriptiveStatistics();
        TopNList topTen = new TopNList(10);

        for (File taskDir : taskDirs) {
            log.info("Processing: " + taskDir);

            Memdrone memdrone = new Memdrone(moduleName, instanceId);
            Map<String, DescriptiveStatistics> taskStats = memdrone.statsByPid();
            Map<String, String> pidMap = memdrone.subTasksByPid();

            Set<String> pids = taskStats.keySet();
            for (String pid : pids) {
                String subTaskName = pidMap.get(pid);
                if (subTaskName == null) {
                    subTaskName = "?:" + pid;
                }

                double max = taskStats.get(pid).getMax();
                memoryStats.addValue(max);
                topTen.add((long) max, subTaskName);
            }
        }

        JFreeChart memHistogram = generateHistogram("Peak Memory Usage", "Memory Usage (MB)",
            "Tasks", arrayToList(memoryStats.getValues(), 1.0 / (1024 * 1024)), 100);

        if (memHistogram != null) {
            pdfRenderer.printChart(memHistogram, CHART3_WIDTH, CHART3_HEIGHT);
        } else {
            pdfRenderer.printText("Histogram: No data points available");
        }

        generateSummaryTable("MATLAB Memory Usage", memoryStats, topTen, new BytesFormat());
    }

    private String shortMetricName(String metricName) {
        String[] elements = metricName.split("\\.");
        String longest = "";
        for (String element : elements) {
            if (element.length() > longest.length()) {
                longest = element;
            }
        }
        return longest;
    }
}
