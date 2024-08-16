package gov.nasa.ziggy.metrics.report;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.jfree.chart.JFreeChart;
import org.jfree.data.category.DefaultCategoryDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lowagie.text.pdf.PdfPTable;

import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetrics;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetrics.Units;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;

public class NodeReport extends Report {
    private static final Logger log = LoggerFactory.getLogger(NodeReport.class);
    private List<String> orderedCategoryNames;
    private Map<String, DescriptiveStatistics> categoryStats;
    private Map<String, TopNList> categoryTopTen;
    private Map<String, Units> categoryUnits;
    private PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();

    public NodeReport(PdfRenderer pdfRenderer) {
        super(pdfRenderer);
    }

    public void generateReport(PipelineInstanceNode node) {
        String moduleName = node.getModuleName();
        pdfRenderer.printText("Pipeline Module: " + moduleName, PdfRenderer.h1Font);

        categoryStats = new HashMap<>();
        categoryTopTen = new HashMap<>();

        Map<String, List<PipelineTaskMetricValue>> categoryMetrics = new HashMap<>();
        categoryUnits = new HashMap<>();

        orderedCategoryNames = new LinkedList<>();

        Map<PipelineTask, List<PipelineTaskMetrics>> taskMetricsByTask = pipelineTaskOperations()
            .taskMetricsByTask(node);

        for (PipelineTask task : taskMetricsByTask.keySet()) {
            for (PipelineTaskMetrics taskMetric : taskMetricsByTask.get(task)) {
                String category = taskMetric.getCategory();

                categoryUnits.put(category, taskMetric.getUnits());

                long value = taskMetric.getValue();

                if (!orderedCategoryNames.contains(category)) {
                    orderedCategoryNames.add(category);
                }

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

                topTen.add(value, "ID: " + task.getId());

                List<PipelineTaskMetricValue> valueList = categoryMetrics.get(category);

                if (valueList == null) {
                    valueList = new ArrayList<>(taskMetricsByTask.size());
                    categoryMetrics.put(category, valueList);
                }

                valueList.add(new PipelineTaskMetricValue(task.getId(), value));
            }
        }

        DefaultCategoryDataset categoryTaskDataset = new DefaultCategoryDataset();

        log.info("summary report");

        for (String category : orderedCategoryNames) {
            log.info("processing category: " + category);

            if (categoryIsTime(category)) {
                List<PipelineTaskMetricValue> values = categoryMetrics.get(category);
                for (PipelineTaskMetricValue value : values) {
                    Long taskId = value.getPipelineTaskId();
                    Long valueMillis = value.getMetricValue();
                    double valueMins = valueMillis / (1000.0 * 60);
                    categoryTaskDataset.addValue(valueMins, category, taskId);
                }
            }
        }

        JFreeChart stackedBar = generateStackedBarChart("Wall Time Breakdown by Task and Category",
            "Tasks", "Time (mins)", categoryTaskDataset);

        pdfRenderer.printChart(stackedBar, CHART2_WIDTH, CHART2_HEIGHT);

        pdfRenderer.newPage();

        // task breakdown table
        pdfRenderer.printText("Wall Time Breakdown by Task and Category", PdfRenderer.h1Font);
        pdfRenderer.println();

        float[] colsWidth = { 1.5f, 1f, 1f, 1f, 1f, 1f, 0.5f };
        PdfPTable breakdownTable = new PdfPTable(colsWidth);
        breakdownTable.setWidthPercentage(100);

        addCell(breakdownTable, "Category", true);
        addCell(breakdownTable, "Mean", true);
        addCell(breakdownTable, "Min", true);
        addCell(breakdownTable, "Max", true);
        addCell(breakdownTable, "StdDev", true);
        addCell(breakdownTable, "90%", true);
        addCell(breakdownTable, "N", true);

        for (String category : orderedCategoryNames) {
            if (categoryIsTime(category)) {
                DescriptiveStatistics stats = categoryStats.get(category);

                addCell(breakdownTable, category);
                addCell(breakdownTable, formatValue(category, stats.getMean()));
                addCell(breakdownTable, formatValue(category, stats.getMin()));
                addCell(breakdownTable, formatValue(category, stats.getMax()));
                addCell(breakdownTable, formatValue(category, stats.getStandardDeviation()));
                addCell(breakdownTable, formatValue(category, stats.getPercentile(90)));
                addCell(breakdownTable, String.format("%d", stats.getN()));
            }
        }

        pdfRenderer.add(breakdownTable);
    }

    protected String formatValue(String category, double value) {
        if (categoryIsTime(category)) {
            return formatTime((long) value);
        }
        return String.format("%.2f", value);
    }

    public boolean categoryIsTime(String category) {
        Units units = categoryUnits.get(category);

        return units != null && units == Units.TIME;
    }

    public List<String> getOrderedCategoryNames() {
        return orderedCategoryNames;
    }

    public Map<String, DescriptiveStatistics> getCategoryStats() {
        return categoryStats;
    }

    public Map<String, TopNList> getCategoryTopTen() {
        return categoryTopTen;
    }

    public Map<String, Units> getCategoryUnits() {
        return categoryUnits;
    }

    PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }

    /**
     * Container for the ID of a {@link PipelineTask} and a metric value.
     *
     * @author PT
     */
    private static class PipelineTaskMetricValue {

        private final long pipelineTaskId;
        private final long metricValue;

        public PipelineTaskMetricValue(long pipelineTaskId, long metricValue) {
            this.pipelineTaskId = pipelineTaskId;
            this.metricValue = metricValue;
        }

        public long getPipelineTaskId() {
            return pipelineTaskId;
        }

        public long getMetricValue() {
            return metricValue;
        }
    }
}
