package gov.nasa.ziggy.report;

import java.awt.Color;
import java.awt.Paint;
import java.text.DecimalFormat;
import java.util.List;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.labels.StandardCategoryItemLabelGenerator;
import org.jfree.chart.labels.StandardCategoryToolTipGenerator;
import org.jfree.chart.labels.StandardPieSectionLabelGenerator;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PiePlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.category.StackedBarRenderer;
import org.jfree.chart.renderer.xy.StandardXYBarPainter;
import org.jfree.chart.renderer.xy.XYBarRenderer;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.general.PieDataset;
import org.jfree.data.statistics.HistogramDataset;
import org.jfree.data.statistics.HistogramType;

import com.lowagie.text.Element;
import com.lowagie.text.Phrase;
import com.lowagie.text.Rectangle;
import com.lowagie.text.pdf.PdfPCell;
import com.lowagie.text.pdf.PdfPTable;
import com.lowagie.text.pdf.RGBColor;

import gov.nasa.ziggy.ui.util.HtmlBuilder;
import gov.nasa.ziggy.util.ZiggyStringUtils;
import gov.nasa.ziggy.util.dispmod.DisplayModel;
import gov.nasa.ziggy.util.dispmod.TaskMetricsDisplayModel;
import gov.nasa.ziggy.util.dispmod.TaskMetricsDisplayModel.MetricDisplayInfo;

/**
 * Base class for top-level elements of the performance report
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
public abstract class Report {
    private static final RGBColor GRAY = new RGBColor(240, 240, 240);

    protected static final int CHART_HEIGHT = 400;
    protected static final int CHART_WIDTH = 700;

    protected PdfRenderer pdfRenderer;

    public Report(PdfRenderer pdfRenderer) {
        this.pdfRenderer = pdfRenderer;
    }

    protected PdfPCell createCell(String s) {
        return createCell(s, false, Element.ALIGN_RIGHT, false, 1);
    }

    protected PdfPCell createCell(String s, boolean bold) {
        return createCell(s, bold, Element.ALIGN_CENTER, false, 1);
    }

    protected PdfPCell createCell(String s, boolean bold, int alignment) {
        return createCell(s, bold, alignment, false, 1);
    }

    protected PdfPCell createCell(String s, int alignment) {
        return createCell(s, false, alignment, false, 1);
    }

    protected PdfPCell createCell(String s, int alignment, boolean oddTableRow) {
        return createCell(s, false, alignment, oddTableRow, 1);
    }

    private PdfPCell createCell(String s, boolean bold, int alignment, boolean oddTableRow,
        int columnSpan) {
        PdfPCell cell = new PdfPCell(
            new Phrase(s, bold ? PdfRenderer.bodyBoldFont : PdfRenderer.bodyFont));

        cell.setHorizontalAlignment(alignment);
        cell.setColspan(columnSpan);
        cell.setBorder(Rectangle.NO_BORDER);

        if (oddTableRow) {
            cell.setBackgroundColor(GRAY);
        }

        return cell;
    }

    private PdfPCell createCell(Phrase phrase) {
        PdfPCell cell = new PdfPCell(phrase);
        cell.setBorder(Rectangle.NO_BORDER);
        return cell;
    }

    private PdfPCell createCell(PdfPTable table) {
        PdfPCell cell = new PdfPCell(table);
        cell.setBorder(Rectangle.NO_BORDER);
        return cell;
    }

    protected int alignment(Class<?> clazz, Object value) {
        if (value.equals(ZiggyStringUtils.NO_DATA)) {
            return Element.ALIGN_CENTER;
        }
        if (Number.class.isAssignableFrom(clazz)) {
            return Element.ALIGN_RIGHT;
        }
        return Element.ALIGN_LEFT;
    }

    protected boolean isOddNumberedTableRow(PdfPTable table) {
        int columnCount = table.getNumberOfColumns();
        int rowCount = table.size();
        if (rowCount == 0) {
            return false;
        }
        int columnsInRow = table.getRow(rowCount - 1).getCells().length;

        // We are in an odd-numbered row if we're still filling in an odd numbered
        // row (i.e., number of cells in the row < number of columns). Alternately,
        // we are in an odd-numbered row if the current row is even-numbered but
        // full, in which case the next cell starts an odd-numbered row.
        return rowCount % 2 == 1 && columnsInRow < columnCount
            || rowCount % 2 == 0 && columnsInRow == columnCount;
    }

    protected void generateSummaryTable(DescriptiveStatistics stats, List<TopNListElement> list) {
        PdfPTable statsTable = new PdfPTable(2);

        Format f = new ReportValueFormat();
        statsTable.addCell(createCell("Mean", true, Element.ALIGN_LEFT));
        statsTable.addCell(createCell(f.format(stats.getMean())));

        statsTable.addCell(createCell("Median", true, Element.ALIGN_LEFT));
        statsTable.addCell(createCell(f.format(stats.getPercentile(50))));

        statsTable.addCell(createCell("Min", true, Element.ALIGN_LEFT));
        statsTable.addCell(createCell(f.format(stats.getMin())));

        statsTable.addCell(createCell("Max", true, Element.ALIGN_LEFT));
        statsTable.addCell(createCell(f.format(stats.getMax())));

        statsTable.addCell(createCell("Sum", true, Element.ALIGN_LEFT));
        statsTable.addCell(createCell(f.format(stats.getSum())));

        statsTable.addCell(createCell("StdDev", true, Element.ALIGN_LEFT));
        statsTable.addCell(createCell(f.format(stats.getStandardDeviation())));

        statsTable.addCell(createCell("90%", true, Element.ALIGN_LEFT));
        statsTable.addCell(createCell(f.format(stats.getPercentile(90))));

        statsTable.addCell(createCell("N", true, Element.ALIGN_LEFT));
        statsTable.addCell(createCell(String.format("%d", stats.getN())));

        PdfPTable layoutTable = new PdfPTable(new float[] { 3.0F, 1.0F, 6.0F });
        layoutTable.setWidthPercentage(85.0F);
        PdfPCell cell = createCell(statsTable);
        cell.setPaddingLeft(CHART_WIDTH * 0.03F);
        layoutTable.addCell(cell);
        layoutTable.addCell(createCell(new Phrase(" ")));
        cell = dumpTopTen(list, f);
        cell.setPaddingRight(CHART_WIDTH * 0.03F);
        layoutTable.addCell(cell);

        pdfRenderer.add(layoutTable);
    }

    private PdfPCell dumpTopTen(List<TopNListElement> list, Format f) {

        PdfPTable topTenTableLeft = new PdfPTable(new float[] { 1.0F, 2.0F, 1.5F });
        PdfPTable topTenTableRight = new PdfPTable(new float[] { 1.0F, 2.0F, 1.5F });

        int[] indexOrder = { 0, 5, 1, 6, 2, 7, 3, 8, 4, 9 };
        boolean left = true; // !left == right

        for (int elementIndex : indexOrder) {
            PdfPTable table = left ? topTenTableLeft : topTenTableRight;
            left = !left;
            if (elementIndex < list.size()) {
                TopNListElement element = list.get(elementIndex);
                table.addCell(createCell(Integer.toString(elementIndex + 1)));
                table.addCell(createCell(element.getLabel()));
                table.addCell(createCell(f.format(element.getValue())));
            } else {
                table.addCell(createCell(" "));
                table.addCell(createCell(" "));
                table.addCell(createCell(" "));
            }
        }

        PdfPTable topTenTable = new PdfPTable(2);
        topTenTable.addCell(createCell("Top 10", true, Element.ALIGN_CENTER, false, 2));
        topTenTable.addCell(createCell(topTenTableLeft));
        topTenTable.addCell(createCell(topTenTableRight));

        return createCell(topTenTable);
    }

    protected JFreeChart generateHistogram(String title, String xCaption, String yCaption,
        List<Double> values, Color color) {

        if (values == null || values.size() == 0) {
            return null;
        }

        // For irregular or pre-defined bin widths, use SimpleHistogramDataset which allows manual
        // bin range definitions.
        HistogramDataset dataset = new HistogramDataset();
        dataset.setType(HistogramType.FREQUENCY);
        dataset.addSeries(yCaption, listToArray(values), binCount(values));

        JFreeChart chart = ChartFactory.createHistogram(title, xCaption, yCaption, dataset,
            PlotOrientation.VERTICAL, true, true, false);
        XYPlot plot = (XYPlot) chart.getPlot();
        plot.setDomainPannable(true);
        plot.setRangePannable(true);
        plot.setForegroundAlpha(0.85f);
        NumberAxis xAxis = (NumberAxis) plot.getDomainAxis();
        xAxis.setNumberFormatOverride(new DecimalFormat("0.####"));
        NumberAxis yAxis = (NumberAxis) plot.getRangeAxis();
        yAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
        XYBarRenderer renderer = (XYBarRenderer) plot.getRenderer();
        renderer.setSeriesPaint(0, color);
        renderer.setBarPainter(new StandardXYBarPainter());
        renderer.setMargin(0.5);
        renderer.setDrawBarOutline(true);
        renderer.setShadowVisible(false);

        return chart;
    }

    private int binCount(List<?> values) {
        // If there are few values that result in a binCount of 2 you usually get a single solid
        // bar. Adding 1 helps mitigate this problem.
        return (int) (2.0 * Math.sqrt(values.size())) + 1;
    }

    protected JFreeChart generateStackedBarChart(String title, String xCaption, String yCaption,
        CategoryDataset dataset) {

        JFreeChart chart = ChartFactory.createStackedBarChart(title, xCaption, yCaption, dataset,
            PlotOrientation.VERTICAL, true, false, false);
        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        StackedBarRenderer renderer = (StackedBarRenderer) plot.getRenderer();
        renderer.setDefaultItemLabelsVisible(true);
        renderer.setDefaultItemLabelGenerator(new StandardCategoryItemLabelGenerator());
        renderer.setDefaultToolTipGenerator(new StandardCategoryToolTipGenerator());
        renderer.setDrawBarOutline(true);

        for (int i = 0; i < dataset.getRowCount(); i++) {
            String rowKey = (String) dataset.getRowKey(i);
            renderer.setSeriesPaint(i, colorForTitle(rowKey));
        }

        return chart;
    }

    private Paint colorForTitle(String rowKey) {
        return TaskMetricsDisplayModel.METRIC_DISPLAY_INFO.stream()
            .filter(m -> rowKey.equals(m.title()))
            .findFirst()
            .orElse(new MetricDisplayInfo(null, null, Color.BLACK))
            .color();
    }

    protected JFreeChart generatePieChart(String title, PieDataset<?> dataset) {
        JFreeChart chart = ChartFactory.createPieChart(title, dataset, true, // include legend
            true, false);

        PiePlot<?> plot = (PiePlot<?>) chart.getPlot();
        StandardPieSectionLabelGenerator labelGenerator = new StandardPieSectionLabelGenerator(
            "{2}");
        plot.setLabelGenerator(labelGenerator);
        plot.setLabelFont(new java.awt.Font("SansSerif", java.awt.Font.PLAIN, 12));
        plot.setNoDataMessage("No data available");
        plot.setCircular(false);
        plot.setLabelGap(0.02);
        return chart;
    }

    protected void printDisplayModel(String title, DisplayModel displayModel) {
        printDisplayModel(title, displayModel, null);
    }

    protected void printDisplayModel(String title, DisplayModel displayModel, float[] colsWidth) {
        pdfRenderer.printText(title, PdfRenderer.h1Font);
        pdfRenderer.println();

        PdfPTable table;

        if (colsWidth != null) {
            table = new PdfPTable(colsWidth);
        } else {
            table = new PdfPTable(displayModel.getColumnCount());
        }

        table.setWidthPercentage(100);

        for (int i = 0; i < displayModel.getColumnCount(); i++) {
            table.addCell(createCell(displayModel.getColumnName(i), true));
        }

        for (int row = 0; row < displayModel.getRowCount(); row++) {
            for (int col = 0; col < displayModel.getColumnCount(); col++) {
                Object value = displayModel.getValueAt(row, col);
                table.addCell(createCell(HtmlBuilder.stripHtml(value.toString()),
                    alignment(displayModel.getColumnClass(col), value),
                    isOddNumberedTableRow(table)));
            }
        }

        pdfRenderer.add(table);
        pdfRenderer.println();
    }

    protected double[] listToArray(List<Double> list) {
        if (list == null || list.size() == 0) {
            return new double[0];
        }

        double[] array = new double[list.size()];
        int index = 0;

        for (double value : list) {
            array[index++] = value;
        }

        return array;
    }
}
