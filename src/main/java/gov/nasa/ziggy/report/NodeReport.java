package gov.nasa.ziggy.report;

import static gov.nasa.ziggy.pipeline.definition.PipelineTaskMetric.Metric.ALGORITHM_TIME;
import static gov.nasa.ziggy.report.HumanReadableStatistics.gigabytesToHumanReadableUnits;
import static gov.nasa.ziggy.report.HumanReadableStatistics.hoursToHumanReadableUnits;

import java.awt.BasicStroke;
import java.awt.Color;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.Marker;
import org.jfree.chart.plot.ValueMarker;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;

import com.lowagie.text.Element;
import com.lowagie.text.pdf.PdfPTable;

import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetric;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetric.Metric;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.pipeline.step.subtask.AlgorithmWallTimes;
import gov.nasa.ziggy.pipeline.step.subtask.AlgorithmWallTimes.SubtaskWallTime;
import gov.nasa.ziggy.report.HumanReadableStatistics.Unit;
import gov.nasa.ziggy.util.ProcessMemoryMonitor;
import gov.nasa.ziggy.util.ProcessMemoryMonitor.MaxMemorySample;
import gov.nasa.ziggy.util.dispmod.TaskMetricsDisplayModel;
import gov.nasa.ziggy.util.dispmod.TaskMetricsDisplayModel.MetricDisplayInfo;
import gov.nasa.ziggy.worker.WorkerResources;
import gov.nasa.ziggy.worker.WorkerResourcesOperations;

public class NodeReport extends Report {

    /**
     * If the data exceeds this margin of a given limit, the x-axis of the plot will be adjusted to
     * show the limit.
     */
    private static final double LIMIT_MARGIN = 0.75;

    private Map<Metric, DescriptiveStatistics> categoryStats = new HashMap<>();
    private Map<Metric, TopNList> categoryTopTen = new HashMap<>();
    private Unit bestCategoryUnit;

    private PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();
    private PipelineTaskDataOperations pipelineTaskDataOperations = new PipelineTaskDataOperations();
    private PipelineInstanceNodeOperations pipelineInstanceNodeOperations = new PipelineInstanceNodeOperations();
    private WorkerResourcesOperations workerResourcesOperations = new WorkerResourcesOperations();

    public NodeReport(PdfRenderer pdfRenderer) {
        super(pdfRenderer);
    }

    public void generateReport(PipelineInstanceNode node) {
        CategoryDataset categoryTaskDataset = gatherMetrics(node);
        if (categoryTaskDataset == null) {
            return;
        }

        createWallTimeBreakdownReport(node, categoryTaskDataset);
        createSubtaskWallTimeBreakdownReport(node);
        createSubtaskMemoryReport(node);
        createCategoryReports(node);
    }

    private CategoryDataset gatherMetrics(PipelineInstanceNode node) {
        Map<Metric, List<PipelineTaskMetricValue>> categoryMetrics = new HashMap<>();
        Map<PipelineTask, List<PipelineTaskMetric>> taskMetricsByTask = pipelineTaskDataOperations()
            .taskMetricsByTask(node);

        for (PipelineTask task : taskMetricsByTask.keySet()) {
            for (PipelineTaskMetric taskMetric : taskMetricsByTask.get(task)) {
                Metric category = taskMetric.getMetric();

                long value = taskMetric.getValue();

                DescriptiveStatistics stats = categoryStats.get(category);

                if (stats == null) {
                    stats = new DescriptiveStatistics();
                    categoryStats.put(category, stats);
                }

                stats.addValue(value);

                TopNList topTen = categoryTopTen.get(category);

                if (topTen == null) {
                    topTen = new TopNList(10);
                    categoryTopTen.put(category, topTen);
                }

                topTen.add(value, task.toString());

                List<PipelineTaskMetricValue> valueList = categoryMetrics.get(category);

                if (valueList == null) {
                    valueList = new ArrayList<>(taskMetricsByTask.size());
                    categoryMetrics.put(category, valueList);
                }

                valueList.add(new PipelineTaskMetricValue(task, value));
            }
        }

        if (categoryMetrics.isEmpty()) {
            return null;
        }

        // Determine best unit for the dataset, which only consists of categories relating to time.
        List<Double> allValues = new ArrayList<>();
        List<Metric> metrics = categoryMetrics.keySet()
            .stream()
            .filter(this::categoryIsTime)
            .toList();
        for (Metric metric : metrics) {
            allValues.addAll(categoryMetrics.get(metric)
                .stream()
                .map(PipelineTaskMetricValue::getMetricValue)
                .map(Long::doubleValue)
                .toList());
        }
        DescriptiveStatistics stats = new DescriptiveStatistics(
            allValues.stream().mapToDouble(Double::doubleValue).toArray());
        bestCategoryUnit = HumanReadableStatistics.millisToHumanReadable(stats).getUnit();

        DefaultCategoryDataset categoryTaskDataset = new DefaultCategoryDataset();
        for (MetricDisplayInfo metricDisplayInfo : TaskMetricsDisplayModel.METRIC_DISPLAY_INFO) {
            Metric category = metricDisplayInfo.metric();
            List<PipelineTaskMetricValue> values = categoryMetrics.get(category);
            if (category == null || values == null) {
                continue;
            }
            if (categoryIsTime(category)) {
                for (PipelineTaskMetricValue value : values) {
                    categoryTaskDataset.addValue(
                        HumanReadableStatistics.millisToHumanReadableUnits(value.getMetricValue(),
                            bestCategoryUnit),
                        metricDisplayInfo.title(), value.getPipelineTask().getId());
                }
            }
        }

        return categoryTaskDataset;
    }

    private void createWallTimeBreakdownReport(PipelineInstanceNode node,
        CategoryDataset categoryTaskDataset) {

        JFreeChart stackedBar = generateStackedBarChart(
            node.getPipelineStepName() + ": Wall time breakdown by task and category", "Tasks",
            "Time (" + bestCategoryUnit + ")", categoryTaskDataset);
        pdfRenderer.printChart(stackedBar, CHART_WIDTH, CHART_HEIGHT);
        pdfRenderer.println();

        float[] colsWidth = { 1.5f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 0.5f };
        PdfPTable breakdownTable = new PdfPTable(colsWidth);
        breakdownTable.setWidthPercentage(100);

        breakdownTable.addCell(createCell("Category", true, Element.ALIGN_LEFT));
        breakdownTable.addCell(createCell("Mean", true, Element.ALIGN_RIGHT));
        breakdownTable.addCell(createCell("Min", true, Element.ALIGN_RIGHT));
        breakdownTable.addCell(createCell("Max", true, Element.ALIGN_RIGHT));
        breakdownTable.addCell(createCell("StdDev", true, Element.ALIGN_RIGHT));
        breakdownTable.addCell(createCell("90%", true, Element.ALIGN_RIGHT));
        breakdownTable.addCell(createCell("N", true, Element.ALIGN_RIGHT));

        for (MetricDisplayInfo metricDisplayInfo : TaskMetricsDisplayModel.METRIC_DISPLAY_INFO) {
            Metric category = metricDisplayInfo.metric();
            if (category == null || !categoryIsTime(category)
                || categoryStats.get(category) == null) {
                continue;
            }
            HumanReadableStatistics humanReadableStatistics = HumanReadableStatistics
                .millisToHumanReadable(categoryStats.get(category));
            DescriptiveStatistics stats = humanReadableStatistics.getStatistics();

            Format f = new ReportValueFormat();
            breakdownTable.addCell(createCell(
                metricDisplayInfo.title() + " (" + humanReadableStatistics.getUnit() + ")",
                Element.ALIGN_LEFT));
            breakdownTable.addCell(createCell(f.format(stats.getMean())));
            breakdownTable.addCell(createCell(f.format(stats.getMin())));
            breakdownTable.addCell(createCell(f.format(stats.getMax())));
            breakdownTable.addCell(createCell(f.format(stats.getStandardDeviation())));
            breakdownTable.addCell(createCell(f.format(stats.getPercentile(90))));
            breakdownTable.addCell(createCell(Long.toString(stats.getN())));
        }
        pdfRenderer.add(breakdownTable);
        pdfRenderer.newPage();
    }

    private void createSubtaskWallTimeBreakdownReport(PipelineInstanceNode node) {

        DescriptiveStatistics stats = new DescriptiveStatistics();
        TopNList topTen = new TopNList(10);
        double maxWallTime = -1.0;
        double typicalWallTime = -1.0;

        for (PipelineTask task : pipelineInstanceNodeOperations().pipelineTasks(List.of(node))) {
            List<SubtaskWallTime> subtaskWallTimes = AlgorithmWallTimes
                .readSubtaskWallTimesFile(task)
                .subtaskWallTimes();
            for (SubtaskWallTime subtaskWallTime : subtaskWallTimes) {
                String subtask = String.format("%d:%d", task.getId(),
                    subtaskWallTime.getSubtaskIndex());
                long wallTimeMillis = subtaskWallTime.getWallTimeMillis();
                stats.addValue(wallTimeMillis);
                topTen.add(wallTimeMillis, subtask);
            }
            if (maxWallTime < 0) {
                PipelineNodeExecutionResources executionResources = pipelineTaskOperations()
                    .executionResources(task);
                maxWallTime = executionResources.getSubtaskMaxWallTimeHours();
                typicalWallTime = executionResources.getSubtaskTypicalWallTimeHours();
            }
        }

        HumanReadableStatistics humanReadableStatistics = HumanReadableStatistics
            .millisToHumanReadable(stats);
        String label = node.getPipelineStepName() + ": Subtask wall time";
        Unit unit = humanReadableStatistics.getUnit();
        String unitsLabel = "Time (" + unit + ")";

        JFreeChart histogram = generateHistogram(label, unitsLabel, "Subtasks",
            humanReadableStatistics.getValues(), categoryColor(ALGORITHM_TIME));
        if (maxWallTime > 0) {
            addMarker(histogram, "Max", hoursToHumanReadableUnits(maxWallTime, unit));
            addMarker(histogram, "Typical", hoursToHumanReadableUnits(typicalWallTime, unit));
            adjustXAxis(histogram, Math.min(typicalWallTime, maxWallTime),
                Math.max(typicalWallTime, maxWallTime), unit);
        }

        pdfRenderer.printChart(histogram, CHART_WIDTH, CHART_HEIGHT);
        pdfRenderer.println();

        generateSummaryTable(humanReadableStatistics.getStatistics(),
            topTen.millisToHumanReadable());
        pdfRenderer.newPage();
    }

    private Color categoryColor(Metric category) {
        MetricDisplayInfo displayInfo = TaskMetricsDisplayModel.METRIC_DISPLAY_INFO.stream()
            .filter(m -> m.metric() == category)
            .findFirst()
            .orElse(new MetricDisplayInfo(null, "Unknown", Color.BLACK));
        return displayInfo.color();
    }

    private void addMarker(JFreeChart histogram, String label, double value) {
        XYPlot plot = (XYPlot) histogram.getPlot();
        Marker marker = new ValueMarker(value);
        marker.setPaint(Color.RED);
        marker.setStroke(new BasicStroke(0.5f, BasicStroke.CAP_BUTT, BasicStroke.JOIN_MITER, 10,
            new float[] { 10f, 10f }, 0));
        marker.setLabel(label);
        plot.addDomainMarker(marker);
    }

    /**
     * Ensure the X axis is at least as long as the given value plus 5% to allow room for labels at
     * the edges. To avoid making the x-axis much larger than the actual data, only change the axis
     * if the maximum value in the data is greater than 75% of the given value. The lower bound will
     * always be used if is less than the given data to show the exceeded limits.
     *
     * @param histogram histogram to adjust
     * @param upperBound value (hours or gigabytes) that should be shown as long as data is within
     * 75% of the value
     * @param lowerBound value that should always be shown
     * @param unit the displayed unit
     */
    private void adjustXAxis(JFreeChart histogram, double lowerBound, double upperBound,
        Unit unit) {

        double adjustedLowerBound = unit.isTime() ? hoursToHumanReadableUnits(lowerBound, unit)
            : gigabytesToHumanReadableUnits(lowerBound, unit);
        double adjustedUpperBound = unit.isTime() ? hoursToHumanReadableUnits(upperBound, unit)
            : gigabytesToHumanReadableUnits(upperBound, unit);

        XYPlot plot = (XYPlot) histogram.getPlot();
        ValueAxis xAxis = plot.getDomainAxis();

        if (adjustedLowerBound > xAxis.getLowerBound()
            && adjustedUpperBound * LIMIT_MARGIN > xAxis.getUpperBound()) {
            return;
        }

        // Add 5% to prevent labels from getting truncated.
        double range = Math.max(adjustedUpperBound, xAxis.getUpperBound())
            - Math.min(adjustedLowerBound, xAxis.getLowerBound());
        double lowerValuePlusMargin = adjustedLowerBound - range * 0.05;
        double upperValuePlusMargin = adjustedUpperBound + range * 0.05;

        if (xAxis.getLowerBound() > lowerValuePlusMargin) {
            xAxis.setLowerBound(lowerValuePlusMargin);
        }

        if (xAxis.getUpperBound() < upperValuePlusMargin) {
            xAxis.setUpperBound(upperValuePlusMargin);
        }
    }

    private void createSubtaskMemoryReport(PipelineInstanceNode node) {

        List<MaxMemorySample> memorySamples = ProcessMemoryMonitor.maxMemorySamplesDescendingOrder(
            pipelineInstanceNodeOperations().pipelineTasks(List.of(node)));

        // Disable this page if memory monitoring was disabled at the time this task was run.
        if (memorySamples.isEmpty()) {
            return;
        }

        DescriptiveStatistics stats = new DescriptiveStatistics();
        TopNList topTen = new TopNList(10);
        double heapSizeGigabytes = -1.0;
        double subtaskRamGigabytes = -1.0;

        for (MaxMemorySample memorySample : memorySamples) {
            String subtask = String.format("%d:%d", memorySample.getPipelineTaskId(),
                memorySample.getSubtaskIndex());
            long memoryUsageBytes = memorySample.getMaxMemorySample().getMemoryUsageBytes();
            stats.addValue(memoryUsageBytes);
            topTen.add(memoryUsageBytes, subtask);

            if (subtaskRamGigabytes < 0) {
                WorkerResources workerResources = workerResourcesOperations()
                    .compositeWorkerResources(pipelineTaskOperations().pipelineNode(
                        pipelineTaskOperations().pipelineTask(memorySample.getPipelineTaskId())));
                heapSizeGigabytes = workerResources.getHeapSizeGigabytes()
                    / workerResources.getMaxWorkerCount();
                PipelineNodeExecutionResources executionResources = pipelineTaskOperations()
                    .executionResources(
                        pipelineTaskOperations().pipelineTask(memorySample.getPipelineTaskId()));
                subtaskRamGigabytes = executionResources.getSubtaskRamGigabytes();
            }
        }

        HumanReadableStatistics humanReadableStatistics = HumanReadableStatistics
            .bytesToHumanReadable(stats);
        Unit unit = humanReadableStatistics.getUnit();
        String label = node.getPipelineStepName() + ": Subtask memory usage";
        String unitsLabel = "Size (" + unit + ")";
        JFreeChart histogram = generateHistogram(label, unitsLabel, "Subtasks",
            humanReadableStatistics.getValues(), categoryColor(ALGORITHM_TIME));
        double upperBound = 0;
        double lowerBound = 0;
        if (heapSizeGigabytes > 0) {
            addMarker(histogram, "Heap/worker",
                gigabytesToHumanReadableUnits(heapSizeGigabytes, unit));
            upperBound = heapSizeGigabytes;
            lowerBound = heapSizeGigabytes;
        }
        if (subtaskRamGigabytes > 0) {
            addMarker(histogram, "Mem/subtask",
                gigabytesToHumanReadableUnits(subtaskRamGigabytes, unit));
            upperBound = Math.max(upperBound, subtaskRamGigabytes);
            lowerBound = Math.min(lowerBound, subtaskRamGigabytes);
        }
        if (upperBound > 0) {
            adjustXAxis(histogram, lowerBound, upperBound, unit);
        }

        pdfRenderer.printChart(histogram, CHART_WIDTH, CHART_HEIGHT);
        pdfRenderer.println();

        generateSummaryTable(humanReadableStatistics.getStatistics(),
            topTen.bytesToHumanReadable());
        pdfRenderer.newPage();
    }

    private void createCategoryReports(PipelineInstanceNode node) {
        for (MetricDisplayInfo metricDisplayInfo : TaskMetricsDisplayModel.METRIC_DISPLAY_INFO) {
            Metric category = metricDisplayInfo.metric();
            if (category == null || categoryStats.get(category) == null) {
                continue;
            }
            CategoryReport categoryReport = new CategoryReport(pdfRenderer, metricDisplayInfo,
                categoryIsTime(category));
            categoryReport.generateReport(node.getPipelineStepName(), categoryStats.get(category),
                categoryTopTen.get(category));
        }
    }

    private boolean categoryIsTime(Metric category) {
        return category.unit() == gov.nasa.ziggy.pipeline.definition.PipelineTaskMetric.Unit.MILLIS;
    }

    PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }

    PipelineTaskDataOperations pipelineTaskDataOperations() {
        return pipelineTaskDataOperations;
    }

    PipelineInstanceNodeOperations pipelineInstanceNodeOperations() {
        return pipelineInstanceNodeOperations;
    }

    WorkerResourcesOperations workerResourcesOperations() {
        return workerResourcesOperations;
    }

    /**
     * Container for the ID of a {@link PipelineTask} and a metric value.
     *
     * @author PT
     */
    private static class PipelineTaskMetricValue {

        private final PipelineTask pipelineTask;
        private final long metricValue;

        public PipelineTaskMetricValue(PipelineTask pipelineTask, long metricValue) {
            this.pipelineTask = pipelineTask;
            this.metricValue = metricValue;
        }

        public PipelineTask getPipelineTask() {
            return pipelineTask;
        }

        public long getMetricValue() {
            return metricValue;
        }
    }
}
