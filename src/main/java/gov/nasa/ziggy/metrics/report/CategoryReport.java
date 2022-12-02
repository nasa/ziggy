package gov.nasa.ziggy.metrics.report;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.jfree.chart.JFreeChart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CategoryReport extends Report {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(CategoryReport.class);

    private final String category;
    private final boolean isTime;

    public CategoryReport(String category, PdfRenderer pdfRenderer, boolean isTime) {
        super(pdfRenderer);
        this.category = category;
        this.isTime = isTime;
    }

    public void generateReport(String moduleName, DescriptiveStatistics stats, TopNList topTen)
        throws Exception {
        String label = moduleName + " : " + category;

        HumanReadableStatistics histogramValues = null;
        String unitsLabel = "";
        Format f;

        if (isTime) {
            histogramValues = millisToHumanReadable(stats);
            unitsLabel = "Time (" + histogramValues.getUnit() + ")";
            f = new TimeMillisFormat();
        } else {
            histogramValues = toHumanReadable(stats, "", 1024.0 * 1024.0);
            unitsLabel = "Size (MB)";
            f = new BytesFormat();
        }

        JFreeChart histogram = generateHistogram(label, unitsLabel, "Tasks",
            histogramValues.getValues(), 20);

        pdfRenderer.printChart(histogram, CHART3_WIDTH, CHART3_HEIGHT);

        pdfRenderer.printText(" ");

        generateSummaryTable(label, stats, topTen, f);

        pdfRenderer.newPage();
    }
}
