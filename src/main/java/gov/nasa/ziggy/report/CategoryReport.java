package gov.nasa.ziggy.report;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.jfree.chart.JFreeChart;

import gov.nasa.ziggy.util.dispmod.TaskMetricsDisplayModel.MetricDisplayInfo;

public class CategoryReport extends Report {

    private final MetricDisplayInfo metricDisplayInfo;
    private final boolean isTime;

    public CategoryReport(PdfRenderer pdfRenderer, MetricDisplayInfo metricDisplayInfo,
        boolean isTime) {
        super(pdfRenderer);
        this.metricDisplayInfo = metricDisplayInfo;
        this.isTime = isTime;
    }

    public void generateReport(String pipelineStepName, DescriptiveStatistics stats,
        TopNList topTen) {

        HumanReadableStatistics humanReadableStatistics;
        TopNList humanReadableTopTen;
        String unitsLabel;

        if (isTime) {
            humanReadableStatistics = HumanReadableStatistics.millisToHumanReadable(stats);
            humanReadableTopTen = topTen
                .toHumanReadable(humanReadableStatistics.getConversion().divisor());
            unitsLabel = "Time (" + humanReadableStatistics.getUnit() + ")";
        } else {
            humanReadableStatistics = HumanReadableStatistics.bytesToHumanReadable(stats);
            humanReadableTopTen = topTen
                .toHumanReadable(humanReadableStatistics.getConversion().divisor());
            unitsLabel = "Size (" + humanReadableStatistics.getUnit() + ")";
        }

        String label = pipelineStepName + ": " + metricDisplayInfo.title();
        JFreeChart histogram = generateHistogram(label, unitsLabel, "Tasks",
            humanReadableStatistics.getValues(), metricDisplayInfo.color());

        pdfRenderer.printChart(histogram, CHART_WIDTH, CHART_HEIGHT);
        pdfRenderer.println();

        generateSummaryTable(humanReadableStatistics.getStatistics(),
            humanReadableTopTen.getList());
        pdfRenderer.newPage();
    }
}
