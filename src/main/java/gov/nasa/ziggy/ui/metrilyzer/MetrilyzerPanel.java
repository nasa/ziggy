package gov.nasa.ziggy.ui.metrilyzer;

import java.awt.BorderLayout;

import gov.nasa.ziggy.services.metrics.MetricsFileParser;

/**
 * Contains the metrics selection panel and the chart panel.
 *
 * @author Todd Klaus
 * @author Sean McCauliff
 */
@SuppressWarnings("serial")
public class MetrilyzerPanel extends javax.swing.JPanel {
    private MetricsSelectorPanel selectorPanel = null;
    private MetricsChartPanel chartPanel = null;
    private final MetricTypeListModel availMetricsModel;
    private final MetricTypeListModel selectedMetricsModel;
    private final MetricsValueSource metricValueSource;

    /**
     * Use the database to get the metrics and their types.
     */
    public MetrilyzerPanel() {
        availMetricsModel = new DatabaseMetricsTypeListModel();
        selectedMetricsModel = new DatabaseMetricsTypeListModel();
        metricValueSource = new DatabaseMetricsValueSource();
        initGUI();
    }

    /**
     * Get metrics from a file.
     *
     * @param metricsFileParser
     */
    public MetrilyzerPanel(MetricsFileParser metricsFileParser) {
        availMetricsModel = new FileSourceMetricsTypeListModel(metricsFileParser);
        selectedMetricsModel = new FileSourceMetricsTypeListModel(metricsFileParser);
        metricValueSource = new FileMetricsValueSource(metricsFileParser);
        initGUI();
    }

    private void initGUI() {
        try {
            BorderLayout thisLayout = new BorderLayout();
            setLayout(thisLayout);
            this.add(getChartPanel(), BorderLayout.CENTER);
            this.add(getSelectorPanel(), BorderLayout.NORTH);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private MetricsSelectorPanel getSelectorPanel() {
        if (selectorPanel == null) {
            selectorPanel = new MetricsSelectorPanel(availMetricsModel, selectedMetricsModel,
                metricValueSource, chartPanel);
        }
        return selectorPanel;
    }

    private MetricsChartPanel getChartPanel() {
        if (chartPanel == null) {
            chartPanel = new MetricsChartPanel();
        }
        return chartPanel;
    }
}
