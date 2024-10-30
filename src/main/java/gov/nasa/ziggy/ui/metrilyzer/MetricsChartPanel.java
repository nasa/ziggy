/*
 *
 */
package gov.nasa.ziggy.ui.metrilyzer;

import java.awt.Color;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.time.TimeSeriesCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.MetricValue;

/**
 * Contains the metrics chart and chart controls. Supports:
 * <ul>
 * <li>add metric to chart
 * <li>remove metric from chart
 * <li>set time window( end time, num samples)
 * </ul>
 *
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class MetricsChartPanel extends ChartPanel {
    private static Logger log = LoggerFactory.getLogger(MetricsChartPanel.class);

    private JFreeChart chart = null;
    private TimeSeriesCollection dataset = new TimeSeriesCollection();

    public MetricsChartPanel() {
        super(ChartFactory.createTimeSeriesChart(null, "Time", "Value", new TimeSeriesCollection(),
            true, true, false));

        chart = getChart();

        XYPlot plot = (XYPlot) chart.getPlot();
        dataset = (TimeSeriesCollection) plot.getDataset();
        XYItemRenderer r = plot.getRenderer();

        if (r instanceof XYLineAndShapeRenderer) {
            XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer) r;
            renderer.setBaseShapesVisible(true);
            renderer.setBaseShapesFilled(true);
        }

        chart.setBackgroundPaint(getBackground());
        plot.setBackgroundPaint(Color.WHITE);
        plot.setDomainGridlinePaint(Color.GRAY);
        plot.setRangeGridlinePaint(Color.GRAY);
    }

    public void clearChart() {
        dataset.removeAllSeries();
    }

//    public void legendVisibility(boolean visible) {
//        // chart.
//    }

    public void addMetric(String name, Collection<MetricValue> metricList, int binSizeMillis) {
        if (metricList == null) {
            log.error("Parameter metricList is null");
            return;
        }

        if (metricList.size() == 0) {
            log.error("Parameter metricList is empty");
            return;
        }

        // partition by hostname
        Map<String, List<MetricValue>> byHost = new HashMap<>();
        for (MetricValue sample : metricList) {
            List<MetricValue> listForHost = byHost.get(sample.getSource());
            if (listForHost == null) {
                listForHost = new LinkedList<>();
                byHost.put(sample.getSource(), listForHost);
            }
            listForHost.add(sample);
        }

        SampleList samples = new SampleList();

        for (List<MetricValue> listForHost : byHost.values()) {
            samples.ingest(listForHost);
        }

        if (binSizeMillis > 0) {
            samples.bin(binSizeMillis);
        }

        log.debug("Adding series[{}] to dataset, {} samples", name, samples.size());
        dataset.addSeries(samples.asTimeSeries(name));
    }
}
